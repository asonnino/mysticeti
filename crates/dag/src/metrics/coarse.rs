// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    CounterVec, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry,
};

use super::names::{
    BENCHMARK_DURATION, BLOCK_HANDLER_CLEANUP_UTIL, BLOCK_STORE_CLEANUP_UTIL, BLOCK_STORE_ENTRIES,
    BLOCK_STORE_LOADED_BLOCKS, BLOCK_STORE_UNLOADED_BLOCKS, BLOCK_SYNC_REQUESTS_RECEIVED,
    BLOCK_SYNC_REQUESTS_SENT, COMMITTED_LEADERS_TOTAL, CORE_LOCK_DEQUEUED, CORE_LOCK_ENQUEUED,
    CORE_LOCK_UTIL, GLOBAL_IN_MEMORY_BLOCKS, GLOBAL_IN_MEMORY_BLOCKS_BYTES, INTER_BLOCK_LATENCY_S,
    LABEL_AUTHORITY, LABEL_COMMIT_TYPE, LABEL_FULFILLED, LABEL_PROC, LABEL_WORKLOAD, LATENCY_S,
    LATENCY_SQUARED_S, LEADER_TIMEOUT_TOTAL, MISSING_BLOCKS, SUBMITTED_TRANSACTIONS,
    UTILIZATION_TIMER, WAL_MAPPINGS,
};

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 0.75, 1., 1.25, 1.5, 1.75, 2., 2.5, 3.0, 4.0, 5., 10., 20., 30., 60., 90.,
];

pub(super) struct CoarseMetrics {
    pub benchmark_duration: IntCounter,
    pub latency_s: HistogramVec,
    pub latency_squared_s: CounterVec,
    pub committed_leaders_total: IntCounterVec,
    pub leader_timeout_total: IntCounter,
    pub inter_block_latency_s: HistogramVec,

    pub block_store_unloaded_blocks: IntCounter,
    pub block_store_loaded_blocks: IntCounter,
    pub block_store_entries: IntCounter,
    pub block_store_cleanup_util: IntCounter,

    pub wal_mappings: IntGauge,

    pub core_lock_util: IntCounter,
    pub core_lock_enqueued: IntCounter,
    pub core_lock_dequeued: IntCounter,

    pub block_handler_cleanup_util: IntCounter,

    pub missing_blocks: IntGaugeVec,
    pub block_sync_requests_sent: IntCounterVec,
    pub block_sync_requests_received: IntCounterVec,

    pub utilization_timer: IntCounterVec,
    pub submitted_transactions: IntCounter,
}

impl CoarseMetrics {
    pub fn new(registry: &Registry) -> Self {
        let metrics = Self {
            benchmark_duration: register_int_counter_with_registry!(
                BENCHMARK_DURATION,
                "Duration of the benchmark",
                registry,
            )
            .unwrap(),
            latency_s: register_histogram_vec_with_registry!(
                LATENCY_S,
                "End-to-end latency of a workload (s)",
                &[LABEL_WORKLOAD],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            latency_squared_s: register_counter_vec_with_registry!(
                LATENCY_SQUARED_S,
                "Square of end-to-end latency (s)",
                &[LABEL_WORKLOAD],
                registry,
            )
            .unwrap(),
            committed_leaders_total: register_int_counter_vec_with_registry!(
                COMMITTED_LEADERS_TOTAL,
                "Committed leaders per authority",
                &[LABEL_AUTHORITY, LABEL_COMMIT_TYPE],
                registry,
            )
            .unwrap(),
            inter_block_latency_s: register_histogram_vec_with_registry!(
                INTER_BLOCK_LATENCY_S,
                "Inter-block latency (s)",
                &[LABEL_WORKLOAD],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            submitted_transactions: register_int_counter_with_registry!(
                SUBMITTED_TRANSACTIONS,
                "Total submitted transactions",
                registry,
            )
            .unwrap(),
            leader_timeout_total: register_int_counter_with_registry!(
                LEADER_TIMEOUT_TOTAL,
                "Total number of leader timeouts",
                registry,
            )
            .unwrap(),
            block_store_loaded_blocks: register_int_counter_with_registry!(
                BLOCK_STORE_LOADED_BLOCKS,
                "Blocks loaded from WAL position",
                registry,
            )
            .unwrap(),
            block_store_unloaded_blocks: register_int_counter_with_registry!(
                BLOCK_STORE_UNLOADED_BLOCKS,
                "Blocks unloaded during cleanup",
                registry,
            )
            .unwrap(),
            block_store_entries: register_int_counter_with_registry!(
                BLOCK_STORE_ENTRIES,
                "Entries in block store",
                registry,
            )
            .unwrap(),
            block_store_cleanup_util: register_int_counter_with_registry!(
                BLOCK_STORE_CLEANUP_UTIL,
                "block_store_cleanup_util",
                registry,
            )
            .unwrap(),
            wal_mappings: register_int_gauge_with_registry!(
                WAL_MAPPINGS,
                "Mappings retained by the WAL",
                registry,
            )
            .unwrap(),
            core_lock_util: register_int_counter_with_registry!(
                CORE_LOCK_UTIL,
                "Utilization of core write lock",
                registry,
            )
            .unwrap(),
            core_lock_enqueued: register_int_counter_with_registry!(
                CORE_LOCK_ENQUEUED,
                "Enqueued core requests",
                registry,
            )
            .unwrap(),
            core_lock_dequeued: register_int_counter_with_registry!(
                CORE_LOCK_DEQUEUED,
                "Dequeued core requests",
                registry,
            )
            .unwrap(),
            block_handler_cleanup_util: register_int_counter_with_registry!(
                BLOCK_HANDLER_CLEANUP_UTIL,
                "block_handler_cleanup_util",
                registry,
            )
            .unwrap(),
            missing_blocks: register_int_gauge_vec_with_registry!(
                MISSING_BLOCKS,
                "Missing blocks per authority",
                &[LABEL_AUTHORITY],
                registry,
            )
            .unwrap(),
            block_sync_requests_sent: register_int_counter_vec_with_registry!(
                BLOCK_SYNC_REQUESTS_SENT,
                "Block sync requests sent",
                &[LABEL_AUTHORITY],
                registry,
            )
            .unwrap(),
            block_sync_requests_received: register_int_counter_vec_with_registry!(
                BLOCK_SYNC_REQUESTS_RECEIVED,
                "Block sync requests received per authority (fulfilled?)",
                &[LABEL_AUTHORITY, LABEL_FULFILLED],
                registry,
            )
            .unwrap(),
            utilization_timer: register_int_counter_vec_with_registry!(
                UTILIZATION_TIMER,
                "Utilization timer",
                &[LABEL_PROC],
                registry,
            )
            .unwrap(),
        };

        crate::data::memory_tracking::init(
            register_int_gauge_with_registry!(
                GLOBAL_IN_MEMORY_BLOCKS,
                "Number of blocks loaded in memory",
                registry,
            )
            .unwrap(),
            register_int_gauge_with_registry!(
                GLOBAL_IN_MEMORY_BLOCKS_BYTES,
                "Total size of blocks in memory",
                registry,
            )
            .unwrap(),
        );

        metrics
    }
}
