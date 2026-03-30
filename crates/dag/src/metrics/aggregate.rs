// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, CounterVec,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 0.75, 1., 1.25, 1.5, 1.75, 2., 2.5, 3.0, 4.0, 5., 10., 20., 30., 60., 90.,
];

/// Metric name constants used by the orchestrator.
pub const BENCHMARK_DURATION: &str = "benchmark_duration";
pub const LATENCY_S: &str = "latency_s";
pub const LATENCY_SQUARED_S: &str = "latency_squared_s";

pub(super) struct AggregateMetrics {
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

impl AggregateMetrics {
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
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            latency_squared_s: register_counter_vec_with_registry!(
                LATENCY_SQUARED_S,
                "Square of end-to-end latency (s)",
                &["workload"],
                registry,
            )
            .unwrap(),
            committed_leaders_total: register_int_counter_vec_with_registry!(
                "committed_leaders_total",
                "Committed leaders per authority",
                &["authority", "commit_type"],
                registry,
            )
            .unwrap(),
            inter_block_latency_s: register_histogram_vec_with_registry!(
                "inter_block_latency_s",
                "Inter-block latency (s)",
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            submitted_transactions: register_int_counter_with_registry!(
                "submitted_transactions",
                "Total submitted transactions",
                registry,
            )
            .unwrap(),
            leader_timeout_total: register_int_counter_with_registry!(
                "leader_timeout_total",
                "Total number of leader timeouts",
                registry,
            )
            .unwrap(),
            block_store_loaded_blocks: register_int_counter_with_registry!(
                "block_store_loaded_blocks",
                "Blocks loaded from WAL position",
                registry,
            )
            .unwrap(),
            block_store_unloaded_blocks: register_int_counter_with_registry!(
                "block_store_unloaded_blocks",
                "Blocks unloaded during cleanup",
                registry,
            )
            .unwrap(),
            block_store_entries: register_int_counter_with_registry!(
                "block_store_entries",
                "Entries in block store",
                registry,
            )
            .unwrap(),
            block_store_cleanup_util: register_int_counter_with_registry!(
                "block_store_cleanup_util",
                "block_store_cleanup_util",
                registry,
            )
            .unwrap(),
            wal_mappings: register_int_gauge_with_registry!(
                "wal_mappings",
                "Mappings retained by the WAL",
                registry,
            )
            .unwrap(),
            core_lock_util: register_int_counter_with_registry!(
                "core_lock_util",
                "Utilization of core write lock",
                registry,
            )
            .unwrap(),
            core_lock_enqueued: register_int_counter_with_registry!(
                "core_lock_enqueued",
                "Enqueued core requests",
                registry,
            )
            .unwrap(),
            core_lock_dequeued: register_int_counter_with_registry!(
                "core_lock_dequeued",
                "Dequeued core requests",
                registry,
            )
            .unwrap(),
            block_handler_cleanup_util: register_int_counter_with_registry!(
                "block_handler_cleanup_util",
                "block_handler_cleanup_util",
                registry,
            )
            .unwrap(),
            missing_blocks: register_int_gauge_vec_with_registry!(
                "missing_blocks",
                "Missing blocks per authority",
                &["authority"],
                registry,
            )
            .unwrap(),
            block_sync_requests_sent: register_int_counter_vec_with_registry!(
                "block_sync_requests_sent",
                "Block sync requests sent",
                &["authority"],
                registry,
            )
            .unwrap(),
            block_sync_requests_received: register_int_counter_vec_with_registry!(
                "block_sync_requests_received",
                "Block sync requests received per authority (fulfilled?)",
                &["authority", "fulfilled"],
                registry,
            )
            .unwrap(),
            utilization_timer: register_int_counter_vec_with_registry!(
                "utilization_timer",
                "Utilization timer",
                &["proc"],
                registry,
            )
            .unwrap(),
        };

        crate::data::init_memory_gauges(
            register_int_gauge_with_registry!(
                "global_in_memory_blocks",
                "Number of blocks loaded in memory",
                registry,
            )
            .unwrap(),
            register_int_gauge_with_registry!(
                "global_in_memory_blocks_bytes",
                "Total size of blocks in memory",
                registry,
            )
            .unwrap(),
        );

        metrics
    }
}
