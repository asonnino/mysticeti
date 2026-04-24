// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use super::{MetricsSnapshot, names::LATENCY_S};
use crate::authority::Authority;

/// Cross-replica aggregations over a slice of [`MetricsSnapshot`]s.
/// Each snapshot is indexed by authority (position `i` = authority `i`),
/// matching the order snapshots are typically collected in a
/// simulation run or a production scrape.
pub struct AggregateMetrics<'a> {
    snapshots: &'a [MetricsSnapshot],
}

impl<'a> AggregateMetrics<'a> {
    pub fn new(snapshots: &'a [MetricsSnapshot]) -> Self {
        Self { snapshots }
    }

    /// 50th-percentile end-to-end transaction latency (ms), averaged across per-replica values.
    pub fn p50_latency_ms(&self) -> Option<f64> {
        self.latency_percentile_ms(0.5)
    }

    /// 90th-percentile end-to-end transaction latency (ms), averaged across per-replica values.
    pub fn p90_latency_ms(&self) -> Option<f64> {
        self.latency_percentile_ms(0.9)
    }

    fn latency_percentile_ms(&self, p: f64) -> Option<f64> {
        let per_replica: Vec<f64> = self
            .snapshots
            .iter()
            .filter_map(|s| s.latency_percentile_ms(p))
            .collect();
        if per_replica.is_empty() {
            None
        } else {
            Some(per_replica.iter().sum::<f64>() / per_replica.len() as f64)
        }
    }

    /// True if any replica reports one or more missing blocks from any peer.
    pub fn any_missing_blocks(&self) -> bool {
        self.snapshots
            .iter()
            .enumerate()
            .any(|(i, snapshot)| snapshot.missing_blocks(Authority::from(i)) > 0)
    }

    /// Committed leaders per second, averaged across replicas. Each replica contributes
    /// its observed total committed-leader sequence length across all authorities
    /// (commits only). Returns `None` when `duration` is zero or nothing committed
    /// anywhere.
    pub fn leader_commits_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let totals: Vec<u64> = self
            .snapshots
            .iter()
            .map(MetricsSnapshot::total_committed_leaders)
            .collect();
        if totals.is_empty() {
            return None;
        }
        let mean = totals.iter().sum::<u64>() as f64 / totals.len() as f64;
        (mean > 0.0).then(|| mean / duration.as_secs_f64())
    }

    /// Committed transactions per second, averaged across replicas.
    /// Each replica's `latency_s` sample count equals the number of
    /// transactions it has observed committed. Returns `None` when no
    /// transactions committed or `duration` is zero.
    pub fn committed_tps(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let counts: Vec<u64> = self
            .snapshots
            .iter()
            .filter_map(|s| s.histogram_sum_and_count(LATENCY_S))
            .map(|(_, count)| count)
            .collect();
        if counts.is_empty() {
            return None;
        }
        let mean = counts.iter().sum::<u64>() as f64 / counts.len() as f64;
        (mean > 0.0).then(|| mean / duration.as_secs_f64())
    }
}
