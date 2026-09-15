// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    Counter, Histogram, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
    register_counter_vec_with_registry, register_counter_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry,
};

use super::names::{
    BENCHMARK_DURATION, BLOCK_HANDLER_CLEANUP_UTIL, BLOCK_LATENCY_S, BLOCK_LATENCY_SQUARED_S,
    BLOCK_STORE_CLEANUP_UTIL, BLOCK_STORE_ENTRIES, BLOCK_STORE_LOADED_BLOCKS,
    BLOCK_STORE_UNLOADED_BLOCKS, BLOCK_SYNC_REQUESTS_RECEIVED, BLOCK_SYNC_REQUESTS_SENT, BlockKind,
    COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_FAST_COMMIT, COMMIT_TYPE_INDIRECT_COMMIT_CERTIFICATE,
    COMMIT_TYPE_INDIRECT_COMMIT_WEAK, COMMIT_TYPE_INDIRECT_SKIP, COMMIT_TYPE_SLOW_COMMIT,
    COMMITTED_LEADERS_TOTAL, CORE_LOCK_DEQUEUED, CORE_LOCK_ENQUEUED, CORE_LOCK_UTIL,
    GLOBAL_IN_MEMORY_BLOCKS, GLOBAL_IN_MEMORY_BLOCKS_BYTES, INTER_BLOCK_LATENCY_S, LABEL_AUTHORITY,
    LABEL_COMMIT_TYPE, LABEL_FULFILLED, LABEL_KIND, LABEL_PROC, LATENCY_S, LATENCY_SQUARED_S,
    LEADER_TIMEOUT_TOTAL, MISSING_BLOCKS, SUBMITTED_TRANSACTIONS, UTILIZATION_TIMER, WAL_MAPPINGS,
};
use crate::authority::Authority;

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.2, 0.3, 0.35, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1., 1.25, 1.5, 1.75, 2., 3.0, 5., 10.,
];

/// A latency histogram paired with the running sum of its squared observations, so mean
/// and standard deviation stay available when the buckets are too coarse.
pub(super) struct LatencyMetrics {
    pub latency_s: Histogram,
    pub latency_squared_s: Counter,
}

impl LatencyMetrics {
    pub fn observe(&self, seconds: f64) {
        self.latency_s.observe(seconds);
        self.latency_squared_s.inc_by(seconds * seconds);
    }
}

/// The `committed_leaders_total` series of one leader authority, one per `commit_type`.
pub(super) struct DecidedLeaderCounters {
    pub fast_commit: IntCounter,
    pub slow_commit: IntCounter,
    pub indirect_commit_certificate: IntCounter,
    pub indirect_commit_weak: IntCounter,
    pub direct_skip: IntCounter,
    pub indirect_skip: IntCounter,
}

pub(super) struct CoarseMetrics {
    pub benchmark_duration: IntCounter,
    /// Submission-to-commit latency of every committed transaction.
    pub transaction_latency: LatencyMetrics,
    /// Proposal-to-commit latency of every committed block, split by `kind`. Both series are
    /// resolved once here so the commit path never hashes label values.
    pub leader_block_latency: LatencyMetrics,
    pub non_leader_block_latency: LatencyMetrics,
    /// Decided leaders, indexed by leader authority. Resolved once here so deciding a leader
    /// neither allocates nor hashes label values.
    pub committed_leaders: Vec<DecidedLeaderCounters>,
    pub leader_timeout_total: IntCounter,
    pub inter_block_latency_s: Histogram,

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
    pub fn new(registry: &Registry, committee_size: usize) -> Self {
        let committed_leaders_total = register_int_counter_vec_with_registry!(
            COMMITTED_LEADERS_TOTAL,
            "Committed leaders per authority",
            &[LABEL_AUTHORITY, LABEL_COMMIT_TYPE],
            registry,
        )
        .unwrap();
        let committed_leaders = (0..committee_size)
            .map(|authority| {
                let authority = Authority::from(authority).to_string();
                let counter = |commit_type: &str| {
                    committed_leaders_total.with_label_values(&[&authority, commit_type])
                };
                DecidedLeaderCounters {
                    fast_commit: counter(COMMIT_TYPE_FAST_COMMIT),
                    slow_commit: counter(COMMIT_TYPE_SLOW_COMMIT),
                    indirect_commit_certificate: counter(COMMIT_TYPE_INDIRECT_COMMIT_CERTIFICATE),
                    indirect_commit_weak: counter(COMMIT_TYPE_INDIRECT_COMMIT_WEAK),
                    direct_skip: counter(COMMIT_TYPE_DIRECT_SKIP),
                    indirect_skip: counter(COMMIT_TYPE_INDIRECT_SKIP),
                }
            })
            .collect();

        let block_latency_s = register_histogram_vec_with_registry!(
            BLOCK_LATENCY_S,
            "Block proposal-to-commit latency (s), subject to cross-replica clock skew",
            &[LABEL_KIND],
            LATENCY_SEC_BUCKETS.to_vec(),
            registry,
        )
        .unwrap();
        let block_latency_squared_s = register_counter_vec_with_registry!(
            BLOCK_LATENCY_SQUARED_S,
            "Square of block proposal-to-commit latency (s)",
            &[LABEL_KIND],
            registry,
        )
        .unwrap();
        let block_latency = |kind: BlockKind| LatencyMetrics {
            latency_s: block_latency_s.with_label_values(&[kind.as_label()]),
            latency_squared_s: block_latency_squared_s.with_label_values(&[kind.as_label()]),
        };

        let metrics = Self {
            benchmark_duration: register_int_counter_with_registry!(
                BENCHMARK_DURATION,
                "Duration of the benchmark",
                registry,
            )
            .unwrap(),
            transaction_latency: LatencyMetrics {
                latency_s: register_histogram_with_registry!(
                    LATENCY_S,
                    "End-to-end transaction commit latency (s)",
                    LATENCY_SEC_BUCKETS.to_vec(),
                    registry,
                )
                .unwrap(),
                latency_squared_s: register_counter_with_registry!(
                    LATENCY_SQUARED_S,
                    "Square of end-to-end transaction commit latency (s)",
                    registry,
                )
                .unwrap(),
            },
            leader_block_latency: block_latency(BlockKind::Leader),
            non_leader_block_latency: block_latency(BlockKind::NonLeader),
            committed_leaders,
            inter_block_latency_s: register_histogram_with_registry!(
                INTER_BLOCK_LATENCY_S,
                "Inter-block latency (s)",
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
