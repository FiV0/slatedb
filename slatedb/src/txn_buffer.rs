//! # Transaction Buffer
//!
//! Internal write buffer used by [`crate::db_transaction::DbTransaction`] to
//! accumulate uncommitted writes. Keeps ops in a [`BTreeMap`] keyed by
//! [`SequencedKey`] so that transaction reads can range-scan the buffer
//! efficiently while the transaction is still open.
//!
//! At commit time the buffer is replayed into a
//! [`crate::batch::WriteBatch`] via [`TxnBuffer::to_write_batch`] and
//! submitted to the DB's write pipeline.

use crate::batch::{WriteBatch, WriteOp};
use crate::config::{MergeOptions, PutOptions};
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::mem_table::{KVTableInternalKeyRange, SequencedKey};
use crate::types::RowEntry;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::RangeBounds;
use uuid::Uuid;

/// Internal buffer of uncommitted transaction writes.
#[derive(Clone, Debug)]
pub(crate) struct TxnBuffer {
    pub(crate) ops: BTreeMap<SequencedKey, WriteOp>,
    pub(crate) txn_id: Uuid,
    /// Tracks the order in which writes happen within a single transaction so
    /// that multiple ops on the same user key (e.g. stacked merges) can
    /// coexist in the [`BTreeMap`] without colliding.
    write_idx: u64,
}

impl TxnBuffer {
    pub(crate) fn new(txn_id: Uuid) -> Self {
        Self {
            ops: BTreeMap::new(),
            txn_id,
            write_idx: 0,
        }
    }

    /// Remove all existing ops for the given user key from the buffer.
    fn remove_ops_by_key(&mut self, key: &Bytes) {
        let start = SequencedKey::new(key.clone(), u64::MAX);
        let end = SequencedKey::new(key.clone(), 0);

        let keys_to_remove: Vec<SequencedKey> = self
            .ops
            .range(start..=end)
            .map(|(k, _)| k.clone())
            .collect();

        for k in keys_to_remove {
            self.ops.remove(&k);
        }
    }

    fn assert_kv<K, V>(&self, key: &K, value: &V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        assert!(!key.is_empty(), "key cannot be empty");
        assert!(
            key.len() <= u16::MAX as usize,
            "key size must be <= u16::MAX"
        );
        assert!(
            value.len() <= u32::MAX as usize,
            "value size must be <= u32::MAX"
        );
    }

    #[cfg(test)]
    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default())
    }

    pub(crate) fn put_with_options<K, V>(&mut self, key: K, value: V, options: &PutOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_bytes_with_options(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
            options,
        )
    }

    pub(crate) fn put_bytes_with_options(
        &mut self,
        key: Bytes,
        value: Bytes,
        options: &PutOptions,
    ) {
        self.assert_kv(&key, &value);

        // put overwrites any prior ops for this key.
        self.remove_ops_by_key(&key);
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Put(key, value, options.clone()),
        );

        self.write_idx += 1;
    }

    #[cfg(test)]
    pub(crate) fn merge<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(key, value, &MergeOptions::default());
    }

    pub(crate) fn merge_with_options<K, V>(&mut self, key: K, value: V, options: &MergeOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.assert_kv(&key, &value);

        let key = Bytes::copy_from_slice(key.as_ref());
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Merge(key, Bytes::copy_from_slice(value.as_ref()), options.clone()),
        );

        self.write_idx += 1;
    }

    pub(crate) fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.assert_kv(&key, &[]);

        let key = Bytes::copy_from_slice(key.as_ref());

        // delete overwrites any prior ops for this key.
        self.remove_ops_by_key(&key);
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Delete(key),
        );

        self.write_idx += 1;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Replay this buffer's ops into a fresh [`WriteBatch`] for submission
    /// to the DB write pipeline.
    pub(crate) fn to_write_batch(&self) -> WriteBatch {
        let mut wb = WriteBatch::new();
        // Iterate in write_idx order so that merge order within each key is
        // preserved when the WriteBatch rebuilds its own per-key layout.
        let mut ops_ordered: Vec<(u64, &WriteOp)> = self
            .ops
            .iter()
            .map(|(sk, op)| (sk.seq, op))
            .collect();
        ops_ordered.sort_by_key(|(idx, _)| *idx);
        for (_, op) in ops_ordered {
            match op {
                WriteOp::Put(k, v, opts) => {
                    wb.put_bytes_with_options(k.clone(), v.clone(), opts);
                }
                WriteOp::Delete(k) => {
                    wb.delete(k.as_ref());
                }
                WriteOp::Merge(k, v, opts) => {
                    wb.merge_with_options(k.as_ref(), v.as_ref(), opts);
                }
            }
        }
        wb.with_txn_id(self.txn_id)
    }
}

/// Iterator over a [`TxnBuffer`]'s entries within a user-key range.
pub(crate) struct TxnBufferIterator {
    iter: Peekable<Box<dyn Iterator<Item = (SequencedKey, RowEntry)> + Send + Sync>>,
    ordering: IterationOrder,
}

impl TxnBufferIterator {
    pub(crate) fn new(
        buf: &TxnBuffer,
        range: impl RangeBounds<Bytes>,
        ordering: IterationOrder,
    ) -> Self {
        let range = KVTableInternalKeyRange::from(range);
        let mut entries: Vec<(SequencedKey, RowEntry)> = buf
            .ops
            .range(range)
            .map(|(k, v)| (k.clone(), v.to_row_entry(u64::MAX, None, None)))
            .collect();

        if matches!(ordering, IterationOrder::Descending) {
            entries.reverse();
        }

        let iter: Box<dyn Iterator<Item = (SequencedKey, RowEntry)> + Send + Sync> =
            Box::new(entries.into_iter());

        Self {
            iter: iter.peekable(),
            ordering,
        }
    }
}

#[async_trait]
impl RowEntryIterator for TxnBufferIterator {
    async fn init(&mut self) -> Result<(), crate::error::SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, crate::error::SlateDBError> {
        Ok(self.iter.next().map(|(_, entry)| entry))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), crate::error::SlateDBError> {
        while let Some((key, _)) = self.iter.peek() {
            if match self.ordering {
                IterationOrder::Ascending => key.user_key.as_ref() < next_key,
                IterationOrder::Descending => key.user_key.as_ref() > next_key,
            } {
                self.iter.next();
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::test_utils::assert_iterator;
    use crate::types::{RowEntry, ValueDeletable};

    fn new_buf() -> TxnBuffer {
        TxnBuffer::new(Uuid::new_v4())
    }

    #[tokio::test]
    async fn test_iterator_basic() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");
        buf.put(b"key2", b"value2");
        buf.delete(b"key4");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new_value(b"key2", b"value2", u64::MAX),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new(
                Bytes::from("key4"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_range() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");
        buf.put(b"key5", b"value5");

        let mut iter = TxnBufferIterator::new(
            &buf,
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        let expected = vec![RowEntry::new_value(b"key3", b"value3", u64::MAX)];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_descending() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");
        buf.put(b"key2", b"value2");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Descending);

        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key2", b"value2", u64::MAX),
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_seek_ascending() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");
        buf.put(b"key5", b"value5");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        iter.seek(b"key3").await.unwrap();

        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key5", b"value5", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_seek_descending() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");
        buf.put(b"key5", b"value5");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Descending);

        iter.seek(b"key3").await.unwrap();

        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_empty_buf() {
        let buf = new_buf();
        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        let result = iter.next().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_iterator_seek_to_nonexistent() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        iter.seek(b"key2").await.unwrap();

        let result = iter.next().await.unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key3"));
        assert_eq!(
            entry.value,
            ValueDeletable::Value(Bytes::from_static(b"value3"))
        );
    }

    #[tokio::test]
    async fn test_iterator_seek_beyond_end() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.put(b"key3", b"value3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        iter.seek(b"key9").await.unwrap();

        let result = iter.next().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_iterator_multiple_tombstones() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.delete(b"key2");
        buf.put(b"key3", b"value3");
        buf.delete(b"key4");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key4"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_iterator_seek_before_first() {
        let mut buf = new_buf();
        buf.put(b"key2", b"value2");
        buf.put(b"key3", b"value3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        iter.seek(b"key1").await.unwrap();

        let result = iter.next().await.unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key2"));
    }

    #[tokio::test]
    async fn test_iterator_range_with_tombstones() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.delete(b"key2");
        buf.put(b"key3", b"value3");
        buf.delete(b"key4");
        buf.put(b"key5", b"value5");

        let mut iter = TxnBufferIterator::new(
            &buf,
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_mixed_operations_including_merges() {
        let mut buf = new_buf();
        buf.put(b"key1", b"value1");
        buf.merge(b"key2", b"merge1");
        buf.merge(b"key2", b"merge2");
        buf.delete(b"key3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key3"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_multiple_merges_for_same_key() {
        let mut buf = new_buf();
        buf.merge(b"key1", b"merge1");
        buf.merge(b"key1", b"merge2");
        buf.merge(b"key1", b"merge3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_merges_in_descending_order() {
        let mut buf = new_buf();
        buf.merge(b"key1", b"merge1");
        buf.merge(b"key2", b"merge2");
        buf.merge(b"key1", b"merge3");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Descending);

        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_merges_in_range() {
        let mut buf = new_buf();
        buf.merge(b"key1", b"merge1");
        buf.merge(b"key3", b"merge3");
        buf.merge(b"key5", b"merge5");

        let mut iter = TxnBufferIterator::new(
            &buf,
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        let expected = vec![RowEntry::new(
            Bytes::from_static(b"key3"),
            ValueDeletable::Merge(Bytes::from_static(b"merge3")),
            u64::MAX,
            None,
            None,
        )];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_seek_to_merge_operations() {
        let mut buf = new_buf();
        buf.merge(b"key1", b"merge1");
        buf.merge(b"key3", b"merge3");
        buf.merge(b"key5", b"merge5");

        let mut iter = TxnBufferIterator::new(&buf, .., IterationOrder::Ascending);
        iter.seek(b"key3").await.unwrap();

        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key3"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key5"),
                ValueDeletable::Merge(Bytes::from_static(b"merge5")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }
}
