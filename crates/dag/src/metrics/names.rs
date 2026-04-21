// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Single source of truth for Prometheus metric names and label identifiers used by this crate.
// Producers (`CoarseMetrics::new`, observation setters) and consumers (`MetricsSnapshot`,
// `AggregateMetrics`) both reference these constants so a rename is a one-line diff and typos
// become compile errors.

use crate::consensus::LeaderStatus;

// Metric names.
pub const BENCHMARK_DURATION: &str = "benchmark_duration";
pub const LATENCY_S: &str = "latency_s";
pub const LATENCY_SQUARED_S: &str = "latency_squared_s";
pub const INTER_BLOCK_LATENCY_S: &str = "inter_block_latency_s";
pub const COMMITTED_LEADERS_TOTAL: &str = "committed_leaders_total";
pub const LEADER_TIMEOUT_TOTAL: &str = "leader_timeout_total";
pub const SUBMITTED_TRANSACTIONS: &str = "submitted_transactions";
pub const MISSING_BLOCKS: &str = "missing_blocks";
pub const BLOCK_SYNC_REQUESTS_SENT: &str = "block_sync_requests_sent";
pub const BLOCK_SYNC_REQUESTS_RECEIVED: &str = "block_sync_requests_received";
pub const BLOCK_STORE_LOADED_BLOCKS: &str = "block_store_loaded_blocks";
pub const BLOCK_STORE_UNLOADED_BLOCKS: &str = "block_store_unloaded_blocks";
pub const BLOCK_STORE_ENTRIES: &str = "block_store_entries";
pub const BLOCK_STORE_CLEANUP_UTIL: &str = "block_store_cleanup_util";
pub const BLOCK_HANDLER_CLEANUP_UTIL: &str = "block_handler_cleanup_util";
pub const CORE_LOCK_UTIL: &str = "core_lock_util";
pub const CORE_LOCK_ENQUEUED: &str = "core_lock_enqueued";
pub const CORE_LOCK_DEQUEUED: &str = "core_lock_dequeued";
pub const WAL_MAPPINGS: &str = "wal_mappings";
pub const UTILIZATION_TIMER: &str = "utilization_timer";
pub const GLOBAL_IN_MEMORY_BLOCKS: &str = "global_in_memory_blocks";
pub const GLOBAL_IN_MEMORY_BLOCKS_BYTES: &str = "global_in_memory_blocks_bytes";

// Label keys.
pub const LABEL_AUTHORITY: &str = "authority";
pub const LABEL_COMMIT_TYPE: &str = "commit_type";
pub const LABEL_WORKLOAD: &str = "workload";
pub const LABEL_FULFILLED: &str = "fulfilled";
pub const LABEL_PROC: &str = "proc";

// Well-known label values (extracted when used in more than one place).
pub const WORKLOAD_SHARED: &str = "shared";

/// Canonical decision outcome for a leader slot, as recorded in the `commit_type` Prometheus
/// label on `committed_leaders_total`. Producers classify a `(LeaderStatus, direct_decide)` pair
/// into one of these four variants; consumers compare against `DecisionType::as_label()` when
/// reading.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionType {
    DirectCommit,
    IndirectCommit,
    DirectSkip,
    IndirectSkip,
}

impl DecisionType {
    /// Canonical string used in the Prometheus `commit_type` label.
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::DirectCommit => "direct-commit",
            Self::IndirectCommit => "indirect-commit",
            Self::DirectSkip => "direct-skip",
            Self::IndirectSkip => "indirect-skip",
        }
    }

    /// Inverse of [`Self::as_label`]. Returns `None` for unknown label values.
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "direct-commit" => Some(Self::DirectCommit),
            "indirect-commit" => Some(Self::IndirectCommit),
            "direct-skip" => Some(Self::DirectSkip),
            "indirect-skip" => Some(Self::IndirectSkip),
            _ => None,
        }
    }

    /// True when this decision committed the leader (as opposed to skipping it).
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::DirectCommit | Self::IndirectCommit)
    }

    /// Classify a `(LeaderStatus, direct_decide)` pair. Returns `None` for `Undecided` — it's not
    /// a decision outcome, so callers should skip metric emission entirely in that case.
    pub fn classify(leader: &LeaderStatus, direct: bool) -> Option<Self> {
        match (leader, direct) {
            (LeaderStatus::Commit(..), true) => Some(Self::DirectCommit),
            (LeaderStatus::Commit(..), false) => Some(Self::IndirectCommit),
            (LeaderStatus::Skip(..), true) => Some(Self::DirectSkip),
            (LeaderStatus::Skip(..), false) => Some(Self::IndirectSkip),
            (LeaderStatus::Undecided(..), _) => None,
        }
    }
}

/// Outcome of a block sync request from a peer, recorded in the `fulfilled` Prometheus label on
/// `block_sync_requests_received`. `Found` = we had the block and served it; `Missing` = we did
/// not and returned `BlockNotFound`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRequestFulfilled {
    Found,
    Missing,
}

impl SyncRequestFulfilled {
    /// Canonical string used in the Prometheus `fulfilled` label.
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Found => "found",
            Self::Missing => "missing",
        }
    }
}

impl From<bool> for SyncRequestFulfilled {
    fn from(found: bool) -> Self {
        if found { Self::Found } else { Self::Missing }
    }
}
