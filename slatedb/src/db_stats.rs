use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper, UpDownCounterFn};
use std::sync::Arc;

use crate::mem_table::KVTableStatsDelta;

pub use crate::merge_operator::MERGE_OPERATOR_OPERANDS;

use crate::merge_operator::{
    MERGE_OPERATOR_FLUSH_PATH, MERGE_OPERATOR_OPERANDS_DESCRIPTION, MERGE_OPERATOR_PATH_LABEL,
    MERGE_OPERATOR_READ_PATH,
};

macro_rules! db_stat_name {
    ($suffix:expr) => {
        concat!("slatedb.db.", $suffix)
    };
}

pub const REQUEST_COUNT: &str = db_stat_name!("request_count");
pub const WRITE_OPS: &str = db_stat_name!("write_ops");
pub const WRITE_BATCH_COUNT: &str = db_stat_name!("write_batch_count");
pub const BACKPRESSURE_COUNT: &str = db_stat_name!("backpressure_count");
pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const WAL_BUFFER_FLUSHES: &str = db_stat_name!("wal_buffer_flushes");
pub const WAL_BUFFER_FLUSH_REQUESTS: &str = db_stat_name!("wal_buffer_flush_requests");
pub const WAL_BUFFER_ESTIMATED_BYTES: &str = db_stat_name!("wal_buffer_estimated_bytes");
pub const TOTAL_MEM_SIZE_BYTES: &str = db_stat_name!("total_mem_size_bytes");
pub const L0_SST_COUNT: &str = db_stat_name!("l0_sst_count");
pub const L0_FLUSH_BYTES: &str = db_stat_name!("l0_flush_bytes");
pub const MEMTABLE_NUM_PUTS: &str = db_stat_name!("memtable_num_puts");
pub const MEMTABLE_NUM_DELETES: &str = db_stat_name!("memtable_num_deletes");
pub const MEMTABLE_NUM_MERGES: &str = db_stat_name!("memtable_num_merges");
pub const MEMTABLE_RAW_KEY_BYTES: &str = db_stat_name!("memtable_raw_key_bytes");
pub const MEMTABLE_RAW_VALUE_BYTES: &str = db_stat_name!("memtable_raw_value_bytes");
pub const SST_FILTER_FALSE_POSITIVE_COUNT: &str = db_stat_name!("sst_filter_false_positive_count");
pub const SST_FILTER_POSITIVE_COUNT: &str = db_stat_name!("sst_filter_positive_count");
pub const SST_FILTER_NEGATIVE_COUNT: &str = db_stat_name!("sst_filter_negative_count");

#[non_exhaustive]
#[derive(Clone)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_estimated_bytes: Arc<dyn GaugeFn>,
    pub(crate) wal_buffer_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_flush_requests: Arc<dyn CounterFn>,
    pub(crate) sst_filter_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_negatives: Arc<dyn CounterFn>,
    pub(crate) backpressure_count: Arc<dyn CounterFn>,
    pub(crate) get_requests: Arc<dyn CounterFn>,
    pub(crate) scan_requests: Arc<dyn CounterFn>,
    pub(crate) flush_requests: Arc<dyn CounterFn>,
    pub(crate) write_batch_count: Arc<dyn CounterFn>,
    pub(crate) write_ops: Arc<dyn CounterFn>,
    pub(crate) total_mem_size_bytes: Arc<dyn GaugeFn>,
    pub(crate) l0_sst_count: Arc<dyn GaugeFn>,
    pub(crate) l0_flush_bytes: Arc<dyn CounterFn>,
    pub(crate) memtable_num_puts: Arc<dyn UpDownCounterFn>,
    pub(crate) memtable_num_deletes: Arc<dyn UpDownCounterFn>,
    pub(crate) memtable_num_merges: Arc<dyn UpDownCounterFn>,
    pub(crate) memtable_raw_key_bytes: Arc<dyn UpDownCounterFn>,
    pub(crate) memtable_raw_value_bytes: Arc<dyn UpDownCounterFn>,
    pub(crate) merge_operator_read_operands: Arc<dyn CounterFn>,
    pub(crate) merge_operator_flush_operands: Arc<dyn CounterFn>,
}

impl DbStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> DbStats {
        DbStats {
            immutable_memtable_flushes: recorder.counter(IMMUTABLE_MEMTABLE_FLUSHES).register(),
            wal_buffer_estimated_bytes: recorder.gauge(WAL_BUFFER_ESTIMATED_BYTES).register(),
            wal_buffer_flushes: recorder.counter(WAL_BUFFER_FLUSHES).register(),
            wal_buffer_flush_requests: recorder.counter(WAL_BUFFER_FLUSH_REQUESTS).register(),
            sst_filter_false_positives: recorder
                .counter(SST_FILTER_FALSE_POSITIVE_COUNT)
                .register(),
            sst_filter_positives: recorder.counter(SST_FILTER_POSITIVE_COUNT).register(),
            sst_filter_negatives: recorder.counter(SST_FILTER_NEGATIVE_COUNT).register(),
            backpressure_count: recorder.counter(BACKPRESSURE_COUNT).register(),
            get_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "get")])
                .register(),
            scan_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "scan")])
                .register(),
            flush_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "flush")])
                .register(),
            write_batch_count: recorder.counter(WRITE_BATCH_COUNT).register(),
            write_ops: recorder.counter(WRITE_OPS).register(),
            total_mem_size_bytes: recorder.gauge(TOTAL_MEM_SIZE_BYTES).register(),
            l0_sst_count: recorder.gauge(L0_SST_COUNT).register(),
            l0_flush_bytes: recorder.counter(L0_FLUSH_BYTES).register(),
            memtable_num_puts: recorder.up_down_counter(MEMTABLE_NUM_PUTS).register(),
            memtable_num_deletes: recorder.up_down_counter(MEMTABLE_NUM_DELETES).register(),
            memtable_num_merges: recorder.up_down_counter(MEMTABLE_NUM_MERGES).register(),
            memtable_raw_key_bytes: recorder.up_down_counter(MEMTABLE_RAW_KEY_BYTES).register(),
            memtable_raw_value_bytes: recorder
                .up_down_counter(MEMTABLE_RAW_VALUE_BYTES)
                .register(),
            merge_operator_read_operands: recorder
                .counter(MERGE_OPERATOR_OPERANDS)
                .labels(&[(MERGE_OPERATOR_PATH_LABEL, MERGE_OPERATOR_READ_PATH)])
                .description(MERGE_OPERATOR_OPERANDS_DESCRIPTION)
                .register(),
            merge_operator_flush_operands: recorder
                .counter(MERGE_OPERATOR_OPERANDS)
                .labels(&[(MERGE_OPERATOR_PATH_LABEL, MERGE_OPERATOR_FLUSH_PATH)])
                .description(MERGE_OPERATOR_OPERANDS_DESCRIPTION)
                .register(),
        }
    }

    pub(crate) fn record_memtable_stats_delta(&self, delta: KVTableStatsDelta) {
        self.memtable_num_puts.increment(delta.num_puts);
        self.memtable_num_deletes.increment(delta.num_deletes);
        self.memtable_num_merges.increment(delta.num_merges);
        self.memtable_raw_key_bytes.increment(delta.raw_key_bytes);
        self.memtable_raw_value_bytes
            .increment(delta.raw_value_bytes);
    }
}
