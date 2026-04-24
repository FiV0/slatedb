//! Background "grim reaper" task that drops values submitted to it from a
//! single task, so expensive `Drop` work (notably dropping the L0 memtable
//! skiplist after an upload, which frees many `Arc<Bytes>` chunks allocated
//! by different tokio workers) happens off the critical path and does not run
//! concurrently with itself on multiple threads.

use std::sync::Mutex;

use async_channel::{Receiver, Sender};
use log::warn;
use tokio::task::JoinHandle;

type ReapBox = Box<dyn Send>;

pub(crate) struct Reaper {
    tx: Sender<ReapBox>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Reaper {
    pub(crate) fn start() -> Self {
        let (tx, rx) = async_channel::unbounded::<ReapBox>();
        let handle = tokio::spawn(Self::run(rx));
        Self {
            tx,
            handle: Mutex::new(Some(handle)),
        }
    }

    async fn run(rx: Receiver<ReapBox>) {
        while let Ok(b) = rx.recv().await {
            drop(b);
        }
    }

    pub(crate) fn submit<T: Send + 'static>(&self, value: T) {
        if let Err(e) = self.tx.try_send(Box::new(value)) {
            drop(e.into_inner());
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.tx.close();
        let handle = self
            .handle
            .lock()
            .expect("reaper handle mutex poisoned")
            .take();
        if let Some(h) = handle {
            if let Err(e) = h.await {
                warn!("reaper task join failed [error={:?}]", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Counter {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Counter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn drains_submitted_values_on_shutdown() {
        let drops = Arc::new(AtomicUsize::new(0));
        let reaper = Reaper::start();
        for _ in 0..64 {
            reaper.submit(Counter {
                drops: drops.clone(),
            });
        }
        reaper.shutdown().await;
        assert_eq!(drops.load(Ordering::SeqCst), 64);
    }

    #[tokio::test]
    async fn submit_after_shutdown_drops_inline() {
        let drops = Arc::new(AtomicUsize::new(0));
        let reaper = Reaper::start();
        reaper.shutdown().await;
        reaper.submit(Counter {
            drops: drops.clone(),
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let reaper = Reaper::start();
        reaper.shutdown().await;
        reaper.shutdown().await;
    }
}
