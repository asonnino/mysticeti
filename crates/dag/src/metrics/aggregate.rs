// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use super::{
    MetricsSnapshot,
    names::{LABEL_WORKLOAD, LATENCY_S, WORKLOAD_SHARED},
};
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

    /// Mean end-to-end transaction latency (ms), averaged across
    /// per-replica means. Returns `None` when no replica observed
    /// any committed transaction (e.g. no load generator).
    pub fn mean_latency_ms(&self) -> Option<f64> {
        let per_replica: Vec<f64> = self
            .snapshots
            .iter()
            .filter_map(MetricsSnapshot::mean_latency_ms)
            .collect();
        if per_replica.is_empty() {
            None
        } else {
            Some(per_replica.iter().sum::<f64>() / per_replica.len() as f64)
        }
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
            .filter_map(|s| {
                s.histogram_percentile(LATENCY_S, &[(LABEL_WORKLOAD, WORKLOAD_SHARED)], p)
            })
            .collect();
        if per_replica.is_empty() {
            None
        } else {
            Some(per_replica.iter().sum::<f64>() / per_replica.len() as f64 * 1000.0)
        }
    }

    /// True if any replica reports one or more missing blocks from any peer.
    pub fn any_missing_blocks(&self) -> bool {
        self.snapshots
            .iter()
            .enumerate()
            .any(|(i, snapshot)| snapshot.missing_blocks(Authority::from(i)) > 0)
    }

    /// Mean committed leaders per second, averaged across replicas. Returns `None` when
    /// `duration` is zero or no replica committed a leader.
    pub fn leader_commits_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let rates: Vec<f64> = self
            .snapshots
            .iter()
            .enumerate()
            .filter_map(|(i, snapshot)| {
                snapshot.leader_commits_per_second(Authority::from(i), duration)
            })
            .collect();
        if rates.is_empty() {
            None
        } else {
            Some(rates.iter().sum::<f64>() / rates.len() as f64)
        }
    }

    /// Committed transactions per second, averaged across replicas.
    /// Each replica's `latency_s` sample count equals the number of
    /// transactions it has observed committed; under consensus those
    /// counts converge, so the mean is the committee's TPS.
    /// Returns `None` when no transactions committed or `duration`
    /// is zero.
    pub fn committed_tps(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let counts: Vec<u64> = self
            .snapshots
            .iter()
            .filter_map(|s| s.histogram_stats(LATENCY_S, &[(LABEL_WORKLOAD, WORKLOAD_SHARED)]))
            .map(|(_, count)| count)
            .collect();
        if counts.is_empty() {
            return None;
        }
        let mean = counts.iter().sum::<u64>() as f64 / counts.len() as f64;
        (mean > 0.0).then(|| mean / duration.as_secs_f64())
    }
}
