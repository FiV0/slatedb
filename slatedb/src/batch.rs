//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

use crate::config::{MergeOptions, PutOptions};
use crate::error::SlateDBError;
use crate::merge_operator::MergeOperatorType;
use crate::types::{RowEntry, ValueDeletable};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// A batch of write operations (puts and/or deletes). All operations in the
/// batch are applied atomically to the database. If multiple operations appear
/// for a a single key, the last operation will be applied. The others will be
/// dropped.
///
/// # Examples
/// ```rust
/// # async fn run() -> Result<(), slatedb::Error> {
/// #     use std::sync::Arc;
/// #     use slatedb::object_store::memory::InMemory;
///     use slatedb::Db;
///     use slatedb::WriteBatch;
///
///     let object_store = Arc::new(InMemory::new());
///     let db = Db::open("path/to/db", object_store).await?;
///
///     let mut batch = WriteBatch::new();
///     batch.put(b"key1", b"value1");
///     batch.put(b"key2", b"value2");
///     batch.delete(b"key3");
///
///     db.write(batch).await;
///     Ok(())
/// # };
/// # tokio_test::block_on(run());
/// ```
///
/// Note that the `WriteBatch` has an unlimited size. This means that batch
/// writes can exceed `l0_sst_size_bytes` (when `WAL` is disabled). It also
/// means that WAL SSTs could get large if there's a large batch write.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    pub(crate) ops: HashMap<Bytes, KeyOps>,
    pub(crate) txn_id: Option<Uuid>,
}

/// Ops accumulated for a single user key in a [`WriteBatch`].
///
/// A `put` or `delete` supersedes any prior ops for that key, so at most one
/// "base" op exists. Merges stack on top of the base (or on top of an empty
/// base) in insertion order.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct KeyOps {
    pub(crate) base: Option<WriteOp>,
    pub(crate) merges: Vec<WriteOp>,
}

impl KeyOps {
    pub(crate) fn total_ops(&self) -> usize {
        self.base.is_some() as usize + self.merges.len()
    }
}

/// A write operation in a batch.
#[derive(PartialEq, Clone)]
pub(crate) enum WriteOp {
    Put(Bytes, Bytes, PutOptions),
    Delete(Bytes),
    Merge(Bytes, Bytes, MergeOptions),
}

impl std::fmt::Debug for WriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn trunc(bytes: &Bytes) -> String {
            if bytes.len() > 10 {
                format!("{:?}...", &bytes[..10])
            } else {
                format!("{:?}", bytes)
            }
        }

        match self {
            WriteOp::Put(key, value, options) => {
                let key = trunc(key);
                let value = trunc(value);
                write!(f, "Put({key}, {value}, {:?})", options)
            }
            WriteOp::Delete(key) => {
                let key = trunc(key);
                write!(f, "Delete({key})")
            }
            WriteOp::Merge(key, value, options) => {
                let key = trunc(key);
                let value = trunc(value);
                write!(f, "Merge({key}, {value}, {:?})", options)
            }
        }
    }
}

impl WriteOp {
    /// Convert WriteOp to RowEntry for queries
    pub(crate) fn to_row_entry(
        &self,
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> RowEntry {
        match self {
            WriteOp::Put(key, value, _options) => {
                // For queries, we don't need to compute expiration time here
                // since these are uncommitted writes. The expiration will be
                // computed when the batch is actually written.
                RowEntry::new(
                    key.clone(),
                    ValueDeletable::Value(value.clone()),
                    seq,
                    create_ts,
                    expire_ts,
                )
            }
            WriteOp::Delete(key) => RowEntry::new(
                key.clone(),
                ValueDeletable::Tombstone,
                seq,
                create_ts,
                expire_ts,
            ),
            WriteOp::Merge(key, value, _options) => RowEntry::new(
                key.clone(),
                ValueDeletable::Merge(value.clone()),
                seq,
                create_ts,
                expire_ts,
            ),
        }
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            ops: HashMap::new(),
            txn_id: None,
        }
    }

    pub(crate) fn with_txn_id(self, txn_id: Uuid) -> Self {
        Self {
            ops: self.ops,
            txn_id: Some(txn_id),
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

    /// Put a key-value pair into the batch. Keys must not be empty.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put_with_options<K, V>(&mut self, key: K, value: V, options: &PutOptions)
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

    /// Put a key-value pair into the batch using owned [`Bytes`], avoiding
    /// the copies that [`WriteBatch::put`] performs via
    /// `Bytes::copy_from_slice`. Prefer this form when the caller already
    /// holds the data as [`Bytes`] (e.g. from a prior read, a zero-copy
    /// buffer pool, or a client that produces [`Bytes`] directly). Keys must
    /// not be empty.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put_bytes(&mut self, key: Bytes, value: Bytes) {
        self.put_bytes_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the batch using owned [`Bytes`] with custom
    /// options. See [`WriteBatch::put_bytes`] for why this form exists.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put_bytes_with_options(&mut self, key: Bytes, value: Bytes, options: &PutOptions) {
        self.assert_kv(&key, &value);

        // put will overwrite the existing key so we can safely
        // remove all previous entries.
        self.ops.insert(
            key.clone(),
            KeyOps {
                base: Some(WriteOp::Put(key, value, options.clone())),
                merges: Vec::new(),
            },
        );
    }

    /// Merge a key-value pair into the batch. Keys must not be empty.
    pub fn merge<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(key, value, &MergeOptions::default());
    }

    /// Merge a key-value pair into the batch with custom options.
    pub fn merge_with_options<K, V>(&mut self, key: K, value: V, options: &MergeOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.assert_kv(&key, &value);

        let key = Bytes::copy_from_slice(key.as_ref());
        let value = Bytes::copy_from_slice(value.as_ref());
        let op = WriteOp::Merge(key.clone(), value, options.clone());
        self.ops.entry(key).or_default().merges.push(op);
    }

    /// Delete a key-value pair into the batch. Keys must not be empty.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.assert_kv(&key, &[]);

        let key = Bytes::copy_from_slice(key.as_ref());

        // delete will overwrite the existing key so we can safely
        // remove all previous entries.
        self.ops.insert(
            key.clone(),
            KeyOps {
                base: Some(WriteOp::Delete(key)),
                merges: Vec::new(),
            },
        );
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Total number of individual write ops (puts + deletes + merges) surviving
    /// in this batch after per-key deduplication.
    pub(crate) fn total_ops(&self) -> usize {
        self.ops.values().map(KeyOps::total_ops).sum()
    }

    pub(crate) fn keys(&self) -> HashSet<Bytes> {
        self.ops.keys().cloned().collect()
    }

    /// Materialize this batch into [`RowEntry`] objects with the given commit
    /// seq and timestamp set. If a merge operator is provided, any stacked
    /// merges for the same key are folded into a single entry. The resulting
    /// `Vec` is unordered — downstream consumers (WAL buffer, memtable) use
    /// `SkipMap` storage that sorts on insert.
    pub(crate) async fn extract_entries(
        &self,
        seq: u64,
        now: i64,
        default_ttl: Option<u64>,
        merger: Option<MergeOperatorType>,
    ) -> Result<Vec<RowEntry>, SlateDBError> {
        let mut entries = Vec::with_capacity(self.ops.len());
        for (user_key, key_ops) in &self.ops {
            if key_ops.merges.is_empty() {
                // Fast path: only a base op (put or delete), no merges.
                if let Some(op) = &key_ops.base {
                    entries.push(row_entry_from_op(op, seq, now, default_ttl));
                }
                continue;
            }

            match &merger {
                Some(merge_op) => {
                    entries.push(fold_key_ops(
                        merge_op.as_ref(),
                        user_key,
                        key_ops,
                        seq,
                        now,
                        default_ttl,
                    )?);
                }
                None => {
                    // No merge operator — emit base + each merge separately.
                    // These will share `seq` and collide in the memtable
                    // SkipMap, but that is a pre-existing shape of the
                    // no-merge-operator-with-merges config.
                    if let Some(op) = &key_ops.base {
                        entries.push(row_entry_from_op(op, seq, now, default_ttl));
                    }
                    for merge in &key_ops.merges {
                        entries.push(row_entry_from_op(merge, seq, now, default_ttl));
                    }
                }
            }
        }
        Ok(entries)
    }
}

/// Build a [`RowEntry`] for a single write op, computing its TTL-derived
/// `expire_ts` from the op's options (if any).
fn row_entry_from_op(op: &WriteOp, seq: u64, now: i64, default_ttl: Option<u64>) -> RowEntry {
    let expire_ts = match op {
        WriteOp::Put(_, _, opts) => opts.expire_ts_from(default_ttl, now),
        WriteOp::Merge(_, _, opts) => opts.expire_ts_from(default_ttl, now),
        WriteOp::Delete(_) => None,
    };
    op.to_row_entry(seq, Some(now), expire_ts)
}

/// Fold a single user key's `base` + stacked `merges` into one [`RowEntry`]
/// using the supplied merge operator. Aggregates the minimum `expire_ts`
/// across the folded ops.
fn fold_key_ops(
    merge_op: &dyn crate::merge_operator::MergeOperator,
    user_key: &Bytes,
    key_ops: &KeyOps,
    seq: u64,
    now: i64,
    default_ttl: Option<u64>,
) -> Result<RowEntry, SlateDBError> {
    let mut min_expire_ts: Option<i64> = None;
    let mut observe = |ts: Option<i64>| {
        if let Some(t) = ts {
            min_expire_ts = Some(match min_expire_ts {
                Some(cur) => cur.min(t),
                None => t,
            });
        }
    };

    // Base value (from a Put) or None (from a Delete, or no base op).
    let base_value: Option<Bytes> = match &key_ops.base {
        Some(WriteOp::Put(_, value, opts)) => {
            observe(opts.expire_ts_from(default_ttl, now));
            Some(value.clone())
        }
        Some(WriteOp::Delete(_)) => None,
        Some(WriteOp::Merge(..)) | None => None,
    };
    let has_base = key_ops.base.is_some();
    let base_is_tombstone = matches!(&key_ops.base, Some(WriteOp::Delete(_)));

    let mut operands: Vec<Bytes> = Vec::with_capacity(key_ops.merges.len());
    for m in &key_ops.merges {
        match m {
            WriteOp::Merge(_, value, opts) => {
                observe(opts.expire_ts_from(default_ttl, now));
                operands.push(value.clone());
            }
            _ => unreachable!("KeyOps::merges contains only WriteOp::Merge variants"),
        }
    }

    let merged = merge_op.merge_batch(user_key, base_value.as_ref().cloned(), &operands)?;

    let value = if has_base && !base_is_tombstone {
        // Base was a Put → result is a concrete value.
        ValueDeletable::Value(merged)
    } else if has_base {
        // Base was a Delete. Per existing `MergeOperatorIterator` semantics
        // (see `merge_operator.rs:437`), merges applied over a tombstone
        // emit a Value (the tombstone's empty state is merged away).
        ValueDeletable::Value(merged)
    } else {
        // No base — the folded result is still a merge operand itself.
        ValueDeletable::Merge(merged)
    };

    Ok(RowEntry {
        key: user_key.clone(),
        value,
        seq,
        create_ts: Some(now),
        expire_ts: min_expire_ts,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::config::Ttl;
    use crate::merge_operator::{MergeOperator, MergeOperatorError};

    struct WriteOpTestCase {
        key: Vec<u8>,
        // None is a delete and options will be ignored
        value: Option<Vec<u8>>,
        options: PutOptions,
    }

    #[rstest]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: Some(b"value".to_vec()),
        options: PutOptions::default(),
    }])]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: None,
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key size must be <= u16::MAX")]
    #[case(vec![WriteOpTestCase {
        key: vec![b'k'; 65_536], // 2^16
        value: None,
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "value size must be <= u32::MAX")]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: Some(vec![b'x'; u32::MAX as usize + 1]), // 2^32
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key cannot be empty")]
    #[case(vec![WriteOpTestCase {
        key: b"".to_vec(),
        value: Some(b"value".to_vec()),
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key cannot be empty")]
    #[case(vec![WriteOpTestCase {
        key: b"".to_vec(),
        value: None,
        options: PutOptions::default(),
    }])]
    fn test_put_delete_batch(#[case] test_case: Vec<WriteOpTestCase>) {
        let mut batch = WriteBatch::new();
        for test_case in test_case.into_iter() {
            let key_bytes = Bytes::from(test_case.key.clone());
            if let Some(value) = test_case.value {
                batch.put_with_options(
                    test_case.key.as_slice(),
                    value.as_slice(),
                    &test_case.options,
                );
                let entry = batch.ops.get(&key_bytes).unwrap();
                assert_eq!(
                    entry.base,
                    Some(WriteOp::Put(
                        key_bytes,
                        Bytes::from(value),
                        test_case.options,
                    )),
                );
                assert!(entry.merges.is_empty());
            } else {
                batch.delete(test_case.key.as_slice());
                let entry = batch.ops.get(&key_bytes).unwrap();
                assert_eq!(entry.base, Some(WriteOp::Delete(key_bytes)));
                assert!(entry.merges.is_empty());
            }
        }
    }

    #[test]
    fn test_writebatch_deduplication() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key1", b"value2"); // should overwrite prior

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();
        match &entry.base {
            Some(WriteOp::Put(_, value, _)) => assert_eq!(value.as_ref(), b"value2"),
            other => panic!("Expected Put base, got {:?}", other),
        }
        assert!(entry.merges.is_empty());
    }

    #[test]
    fn should_create_merge_operation_with_default_options() {
        let mut batch = WriteBatch::new();

        batch.merge(b"key1", b"value1");

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();
        assert!(entry.base.is_none());
        assert_eq!(entry.merges.len(), 1);
        match &entry.merges[0] {
            WriteOp::Merge(key, value, options) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert_eq!(options, &MergeOptions::default());
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_create_merge_operation_with_custom_options() {
        let mut batch = WriteBatch::new();
        let merge_options = MergeOptions {
            ttl: Ttl::ExpireAfter(3600),
        };

        batch.merge_with_options(b"key1", b"value1", &merge_options);

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();
        assert!(entry.base.is_none());
        assert_eq!(entry.merges.len(), 1);
        match &entry.merges[0] {
            WriteOp::Merge(key, value, options) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert_eq!(options.ttl, Ttl::ExpireAfter(3600));
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_allow_multiple_merges_for_same_key() {
        let mut batch = WriteBatch::new();

        batch.merge(b"key1", b"value1");
        batch.merge(b"key1", b"value2");
        batch.merge(b"key1", b"value3");

        // Still one user key in the batch.
        assert_eq!(batch.ops.len(), 1);

        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();
        assert!(entry.base.is_none());
        assert_eq!(entry.merges.len(), 3);
        let values: Vec<&[u8]> = entry
            .merges
            .iter()
            .map(|m| match m {
                WriteOp::Merge(_, v, _) => v.as_ref(),
                _ => panic!("Expected Merge operation"),
            })
            .collect();
        assert_eq!(values, vec![&b"value1"[..], &b"value2"[..], &b"value3"[..]]);
    }

    #[test]
    fn should_allow_merges_for_different_keys() {
        let mut batch = WriteBatch::new();

        batch.merge(b"key1", b"value1");
        batch.merge(b"key2", b"value2");
        batch.merge(b"key3", b"value3");

        assert_eq!(batch.ops.len(), 3);
        assert!(batch.ops.contains_key(&Bytes::from_static(b"key1")));
        assert!(batch.ops.contains_key(&Bytes::from_static(b"key2")));
        assert!(batch.ops.contains_key(&Bytes::from_static(b"key3")));
    }

    #[test]
    fn should_preserve_both_put_and_merge_when_merge_comes_after_put() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"put_value");

        batch.merge(b"key1", b"merge_value");

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();

        match &entry.base {
            Some(WriteOp::Put(key, value, _)) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"put_value");
            }
            other => panic!("Expected Put base, got {:?}", other),
        }
        assert_eq!(entry.merges.len(), 1);
        match &entry.merges[0] {
            WriteOp::Merge(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"merge_value");
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_preserve_both_delete_and_merge_when_merge_comes_after_delete() {
        let mut batch = WriteBatch::new();
        batch.delete(b"key1");

        batch.merge(b"key1", b"merge_value");

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();

        match &entry.base {
            Some(WriteOp::Delete(key)) => {
                assert_eq!(key.as_ref(), b"key1");
            }
            other => panic!("Expected Delete base, got {:?}", other),
        }
        assert_eq!(entry.merges.len(), 1);
        match &entry.merges[0] {
            WriteOp::Merge(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"merge_value");
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_deduplicate_merge_when_put_is_added_for_same_key() {
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge_value");

        batch.put(b"key1", b"put_value");

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();

        assert!(entry.merges.is_empty(), "merge should have been cleared");
        match &entry.base {
            Some(WriteOp::Put(key, value, _)) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"put_value");
            }
            other => panic!("Expected Put base, got {:?}", other),
        }
    }

    #[test]
    fn should_deduplicate_merge_when_delete_is_added_for_same_key() {
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge_value");

        batch.delete(b"key1");

        assert_eq!(batch.ops.len(), 1);
        let entry = batch.ops.get(&Bytes::from_static(b"key1")).unwrap();

        assert!(entry.merges.is_empty(), "merge should have been cleared");
        match &entry.base {
            Some(WriteOp::Delete(key)) => {
                assert_eq!(key.as_ref(), b"key1");
            }
            other => panic!("Expected Delete base, got {:?}", other),
        }
    }

    // -------- extract_entries tests --------

    struct StringConcatMergeOperator;

    impl MergeOperator for StringConcatMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operand: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            match existing_value {
                Some(base) => {
                    let mut merged = base.to_vec();
                    merged.extend_from_slice(&operand);
                    Ok(Bytes::from(merged))
                }
                None => Ok(operand),
            }
        }
    }

    #[tokio::test]
    async fn should_extract_entries_no_merges() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key3");

        let result = batch.extract_entries(100, 1000, None, None).await.unwrap();

        let mut entries = result.into_iter().collect::<Vec<_>>();
        entries.sort_by_key(|e| e.key.clone());

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, Bytes::from_static(b"key1"));
        assert_eq!(
            entries[0].value,
            ValueDeletable::Value(Bytes::from_static(b"value1"))
        );
        assert_eq!(entries[1].key, Bytes::from_static(b"key2"));
        assert_eq!(
            entries[1].value,
            ValueDeletable::Value(Bytes::from_static(b"value2"))
        );
        assert_eq!(entries[2].key, Bytes::from_static(b"key3"));
        assert_eq!(entries[2].value, ValueDeletable::Tombstone);
    }

    #[tokio::test]
    async fn should_extract_entries_multiple_merges() {
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key1", b"merge2");
        batch.merge(b"key1", b"merge3");

        let merge_operator = Some(std::sync::Arc::new(StringConcatMergeOperator)
            as crate::merge_operator::MergeOperatorType);
        let result = batch
            .extract_entries(100, 1000, None, merge_operator)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, Bytes::from_static(b"key1"));
        assert_eq!(
            result[0].value,
            ValueDeletable::Merge(Bytes::from_static(b"merge1merge2merge3"))
        );
    }

    #[tokio::test]
    async fn should_extract_entries_delete_then_merge() {
        let mut batch = WriteBatch::new();
        batch.delete(b"key1");
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key1", b"merge2");

        let merge_operator = Some(std::sync::Arc::new(StringConcatMergeOperator)
            as crate::merge_operator::MergeOperatorType);
        let result = batch
            .extract_entries(100, 1000, None, merge_operator)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, Bytes::from_static(b"key1"));
        // Tombstone base: operands concatenate without any base value,
        // yielding merge1merge2 and emitted as a concrete Value.
        assert_eq!(
            result[0].value,
            ValueDeletable::Value(Bytes::from_static(b"merge1merge2"))
        );
    }

    #[tokio::test]
    async fn should_extract_entries_value_then_merge() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value");
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key1", b"merge2");

        let merge_operator = Some(std::sync::Arc::new(StringConcatMergeOperator)
            as crate::merge_operator::MergeOperatorType);
        let result = batch
            .extract_entries(100, 1000, None, merge_operator)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, Bytes::from_static(b"key1"));
        assert_eq!(
            result[0].value,
            ValueDeletable::Value(Bytes::from_static(b"valuemerge1merge2"))
        );
    }
}
