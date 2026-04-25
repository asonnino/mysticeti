// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Live progress line for long-running runs (e.g. `local-testbed --perpetual`).
//! [`print_progress`] writes a single aggregated line to stderr; the caller
//! drives the cadence (no internal timer / task). One-shot, side-effecting:
//! pure printing, like [`super::spinner`].

use std::time::Duration;

use dag::metrics::{LATENCY_S, MetricsSnapshot};

/// Print one progress line to stderr:
///   `[t=Ns] · committed=N · X.X commits/s · Y tx/s · p50 N ms · p90 N ms`.
/// Sub-fields are omitted when no replica produced data (e.g. before the load
/// generator's `initial_delay` has elapsed, the latency histogram is empty).
pub fn print_progress(elapsed: Duration, snapshots: &[MetricsSnapshot]) {
    let elapsed_secs = elapsed.as_secs();
    let max_committed = snapshots
        .iter()
        .map(|s| s.total_committed_leaders())
        .max()
        .unwrap_or(0);
    let mut parts = vec![
        format!("[t={elapsed_secs}s]"),
        format!("committed={max_committed}"),
    ];
    let elapsed_secs_f = elapsed.as_secs_f64();
    if elapsed_secs_f > 0.0 && !snapshots.is_empty() {
        let mean_committed = snapshots
            .iter()
            .map(|s| s.total_committed_leaders())
            .sum::<u64>() as f64
            / snapshots.len() as f64;
        if mean_committed > 0.0 {
            parts.push(format!("{:.1} commits/s", mean_committed / elapsed_secs_f));
        }
        let tx_counts: Vec<u64> = snapshots
            .iter()
            .filter_map(|s| s.histogram_sum_and_count(LATENCY_S))
            .map(|(_, count)| count)
            .collect();
        if !tx_counts.is_empty() {
            let mean_tx = tx_counts.iter().sum::<u64>() as f64 / tx_counts.len() as f64;
            if mean_tx > 0.0 {
                parts.push(format!("{:.0} tx/s", mean_tx / elapsed_secs_f));
            }
        }
    }
    let p50 = mean_per_replica(snapshots, |s| s.latency_percentile_ms(0.5));
    let p90 = mean_per_replica(snapshots, |s| s.latency_percentile_ms(0.9));
    if let (Some(p50), Some(p90)) = (p50, p90) {
        parts.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
    }
    eprintln!("{}", parts.join(" · "));
}

/// Average a per-replica value, ignoring replicas where the extractor returns
/// `None` (e.g. histograms that haven't seen a sample yet).
fn mean_per_replica<F>(snapshots: &[MetricsSnapshot], extract: F) -> Option<f64>
where
    F: Fn(&MetricsSnapshot) -> Option<f64>,
{
    let values: Vec<f64> = snapshots.iter().filter_map(extract).collect();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}
