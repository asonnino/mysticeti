// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-replica aggregations over a slice of [`MetricsSnapshot`]. The single home for
//! "average / max / per-replica" reductions; both [`super::RunResult`] (final summary)
//! and the live-heartbeat renderer in the cli crate go through here so the same numbers
//! drive both views.

use std::time::Duration;

use super::{MetricsSnapshot, names::LATENCY_S};

/// View over a slice of [`MetricsSnapshot`] that exposes the cross-replica
/// reductions used by the run summary and the live heartbeat. Cheap to construct;
/// every method is `O(replicas)`.
pub struct SnapshotAggregate<'a> {
    snapshots: &'a [MetricsSnapshot],
}

impl<'a> SnapshotAggregate<'a> {
    pub fn new(snapshots: &'a [MetricsSnapshot]) -> Self {
        Self { snapshots }
    }

    /// Per-replica count of committed leaders, in authority order. Derived from each
    /// snapshot's `committed_leaders_total` counter (summed across all leader authorities).
    pub fn committed_leaders_per_replica(&self) -> Vec<usize> {
        self.snapshots
            .iter()
            .map(|s| s.total_committed_leaders() as usize)
            .collect()
    }

    /// Maximum committed-leader count across replicas (best-of view, suitable for the
    /// live heartbeat's `committed=N` field).
    pub fn max_committed_leaders(&self) -> u64 {
        self.snapshots
            .iter()
            .map(MetricsSnapshot::total_committed_leaders)
            .max()
            .unwrap_or(0)
    }

    /// Mean committed-leader count across replicas. `None` when there are no replicas
    /// or every replica reports zero (so callers can omit the field rather than print
    /// a vacuous `0.0 commits/s`).
    pub fn mean_committed_leaders(&self) -> Option<f64> {
        if self.snapshots.is_empty() {
            return None;
        }
        let sum: u64 = self
            .snapshots
            .iter()
            .map(MetricsSnapshot::total_committed_leaders)
            .sum();
        let mean = sum as f64 / self.snapshots.len() as f64;
        (mean > 0.0).then_some(mean)
    }

    /// Mean transactions committed across replicas, taken from each replica's
    /// `latency_s` histogram sample-count (which equals the count of committed
    /// transactions it observed). `None` when no replica has logged any transactions
    /// yet — typically before the load generator's `initial_delay` has elapsed.
    pub fn mean_committed_transactions(&self) -> Option<f64> {
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
        (mean > 0.0).then_some(mean)
    }

    /// `p`-th percentile end-to-end latency (ms), averaged across replicas that have
    /// produced enough histogram samples to compute it. `None` when no replica has
    /// any samples.
    pub fn mean_latency_percentile_ms(&self, p: f64) -> Option<f64> {
        let values: Vec<f64> = self
            .snapshots
            .iter()
            .filter_map(|s| s.latency_percentile_ms(p))
            .collect();
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }

    /// Mean committed-leader rate (leaders/s) across replicas over `duration`.
    /// `None` when `duration` is zero or no replica committed anything.
    pub fn committed_leaders_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        Some(self.mean_committed_leaders()? / duration.as_secs_f64())
    }

    /// Mean committed-transaction rate (TPS) across replicas over `duration`.
    /// `None` when `duration` is zero or no replica observed any transactions.
    pub fn transactions_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        Some(self.mean_committed_transactions()? / duration.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MetricsSnapshot, SnapshotAggregate};
    use crate::authority::Authority;
    use crate::block::Block;
    use crate::block::data::Data;
    use crate::consensus::LeaderStatus;
    use crate::metrics::Metrics;

    fn snapshot_with_n_committed_leaders(n: u64) -> MetricsSnapshot {
        let metrics = Metrics::new_for_test(4);
        for _ in 0..n {
            let block = Block::new_for_test(Authority::from(0_usize), 1, vec![]);
            metrics.inc_decided_leaders(&LeaderStatus::DirectCommit(Data::new(block)));
        }
        metrics.collect()
    }

    fn snapshot_with_latency_samples(samples: &[u64]) -> MetricsSnapshot {
        let metrics = Metrics::new_for_test(4);
        for ms in samples {
            metrics.observe_latency_s(Duration::from_millis(*ms).as_secs_f64());
        }
        metrics.collect()
    }

    #[test]
    fn empty_returns_none_or_zero() {
        let agg = SnapshotAggregate::new(&[]);
        assert_eq!(agg.committed_leaders_per_replica(), Vec::<usize>::new());
        assert_eq!(agg.max_committed_leaders(), 0);
        assert_eq!(agg.mean_committed_leaders(), None);
        assert_eq!(agg.mean_committed_transactions(), None);
        assert_eq!(agg.mean_latency_percentile_ms(0.5), None);
    }

    #[test]
    fn all_zero_committed_leaders_returns_none() {
        let snapshots = vec![
            snapshot_with_n_committed_leaders(0),
            snapshot_with_n_committed_leaders(0),
        ];
        let agg = SnapshotAggregate::new(&snapshots);
        assert_eq!(agg.max_committed_leaders(), 0);
        assert_eq!(agg.mean_committed_leaders(), None);
    }

    #[test]
    fn mean_committed_leaders_averages_across_replicas() {
        let snapshots = vec![
            snapshot_with_n_committed_leaders(10),
            snapshot_with_n_committed_leaders(20),
        ];
        let agg = SnapshotAggregate::new(&snapshots);
        assert_eq!(agg.max_committed_leaders(), 20);
        assert_eq!(agg.mean_committed_leaders(), Some(15.0));
        assert_eq!(agg.committed_leaders_per_replica(), vec![10, 20]);
    }

    #[test]
    fn mean_latency_percentile_ms_averages_two_replicas() {
        let s1 = snapshot_with_latency_samples(&(1..=100).collect::<Vec<u64>>());
        let s2 = snapshot_with_latency_samples(&(101..=200).collect::<Vec<u64>>());
        let p1 = s1.latency_percentile_ms(0.5).unwrap();
        let p2 = s2.latency_percentile_ms(0.5).unwrap();

        let snapshots = vec![s1, s2];
        let agg = SnapshotAggregate::new(&snapshots);
        assert_eq!(agg.mean_latency_percentile_ms(0.5), Some((p1 + p2) / 2.0),);
    }

    #[test]
    fn mean_latency_percentile_ms_filters_replicas_without_samples() {
        let populated = snapshot_with_latency_samples(&(1..=100).collect::<Vec<u64>>());
        let expected = populated.latency_percentile_ms(0.5);
        assert!(
            expected.is_some(),
            "fixture sanity: 100 samples should produce p50"
        );

        let snapshots = vec![snapshot_with_latency_samples(&[]), populated];
        let agg = SnapshotAggregate::new(&snapshots);
        // Empty replicas drop out; the mean equals the one populated replica's value.
        assert_eq!(agg.mean_latency_percentile_ms(0.5), expected);
    }

    #[test]
    fn mean_committed_transactions_none_when_no_histogram_samples() {
        let snapshots = vec![
            snapshot_with_latency_samples(&[]),
            snapshot_with_latency_samples(&[]),
        ];
        let agg = SnapshotAggregate::new(&snapshots);
        assert_eq!(agg.mean_committed_transactions(), None);
    }
}
