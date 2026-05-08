// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Display extension traits used by [`super::Terminal`]. The domain types in `dag`,
//! `simulator`, and `replica` stay plain; every presentation concern (config tables,
//! live progress lines, outcome badges, per-result blocks) lives here so the
//! renderer's knowledge is co-located in one file.

use std::time::Duration;

use dag::metrics::{LATENCY_S, MetricsSnapshot, Outcome, RunResult};
use replica::testbed::TestbedConfig;
use simulator::SimulationConfig;

use super::table::{self, ConfigRow, ReplicaRow};
use super::{GREEN, RED, RESET, YELLOW};

/// Adapter trait letting [`super::Terminal`] render any run config: the per-run
/// heading uses [`Self::name`], the suite summary's `nodes` column uses
/// [`Self::committee_size`], and the per-run config table renders
/// [`Self::config_rows`] (empty means "no config table for this command").
pub trait ConfigRender {
    fn name(&self) -> Option<&str>;
    fn committee_size(&self) -> usize;
    fn config_rows(&self) -> Vec<ConfigRow>;
}

impl ConfigRender for SimulationConfig {
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn committee_size(&self) -> usize {
        self.committee_size
    }

    fn config_rows(&self) -> Vec<ConfigRow> {
        vec![
            ConfigRow::new("Committee size", self.committee_size.to_string()),
            ConfigRow::new("Topology", self.topology.to_string()),
            ConfigRow::new("Duration", format!("{}s", self.duration_secs)),
            ConfigRow::new(
                "Latency range",
                format!("{}-{} ms", self.latency_min_ms, self.latency_max_ms),
            ),
            ConfigRow::new("RNG seed", self.rng_seed.to_string()),
        ]
    }
}

impl ConfigRender for TestbedConfig {
    fn name(&self) -> Option<&str> {
        None
    }

    fn committee_size(&self) -> usize {
        self.committee_size
    }

    fn config_rows(&self) -> Vec<ConfigRow> {
        vec![
            ConfigRow::new("Committee size", self.committee_size.to_string()),
            ConfigRow::new(
                "Tx size",
                format!("{} B", self.load_generator.transaction_size),
            ),
            ConfigRow::new("Load", format!("{} tx/s", self.load_generator.load)),
            ConfigRow::new(
                "Initial delay",
                humantime::format_duration(self.load_generator.initial_delay).to_string(),
            ),
        ]
    }
}

/// Display helpers for the plain [`Outcome`] enum.
pub trait OutcomeDisplay {
    /// Single-character glyph, no colour. Usable standalone (e.g. inside a table cell).
    fn glyph(&self) -> &'static str;
    /// Fully-formatted badge line. When `color=true`, wraps the glyph + prose in ANSI
    /// colour escapes; when `false`, returns `"LABEL: prose"`.
    fn badge(&self, color: bool) -> String;
}

impl OutcomeDisplay for Outcome {
    fn glyph(&self) -> &'static str {
        match self {
            Outcome::Pass => "✓",
            Outcome::NoProgress => "⚠",
            Outcome::Diverged => "✗",
        }
    }

    fn badge(&self, color: bool) -> String {
        let prose = match self {
            Outcome::Pass => "Commits consistent across all replicas",
            Outcome::NoProgress => "Safe but no leader was committed",
            Outcome::Diverged => "Commits DIVERGED across replicas",
        };
        if color {
            let color_code = match self {
                Outcome::Pass => GREEN,
                Outcome::NoProgress => YELLOW,
                Outcome::Diverged => RED,
            };
            format!("{color_code}{glyph} {prose}{RESET}", glyph = self.glyph())
        } else {
            let label = match self {
                Outcome::Pass => "PASS",
                Outcome::NoProgress => "WARN",
                Outcome::Diverged => "FAIL",
            };
            format!("{label}: {prose}")
        }
    }
}

/// Per-result display block: outcome badge followed by either a one-line happy-path
/// summary or the full per-replica table.
pub trait RunResultRender {
    fn render(&self, color: bool) -> String;
}

impl<C> RunResultRender for RunResult<C> {
    fn render(&self, color: bool) -> String {
        let outcome = self.outcome;
        let commit_counts = self.leaders_committed_per_replica();

        let mut out = outcome.badge(color);
        out.push('\n');

        let uniform_commits = commit_counts
            .first()
            .map(|first| commit_counts.iter().all(|c| c == first))
            .unwrap_or(true);
        if outcome != Outcome::Diverged && uniform_commits {
            let committed = commit_counts.first().copied().unwrap_or_default();
            let rate = match self.leaders_committed_per_second() {
                Some(r) => format!("{r:.1} commits/s"),
                None => "— commits/s".into(),
            };
            let mut headline = Vec::new();
            if let (Some(p50), Some(p90)) = (self.p50_latency_ms(), self.p90_latency_ms()) {
                headline.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
            }
            if let Some(tps) = self.transactions_committed_per_second() {
                headline.push(format!("{tps:.0} TPS"));
            }
            let headline = headline.join(" · ");
            if headline.is_empty() {
                out.push_str(&format!("  {committed} commits, {rate}"));
            } else {
                out.push_str(&format!("  {headline} ({committed} commits, {rate})"));
            }
        } else {
            out.push_str(&table::render(ReplicaRow::for_result(self)));
        }
        out
    }
}

/// Live one-line aggregate of the running replicas, e.g.
/// `[t=Ns] · committed=N · X.X commits/s · Y tx/s · p50 N ms · p90 N ms`.
/// Sub-fields are omitted when no replica produced data yet (e.g. before the
/// load generator's `initial_delay` has elapsed and the latency histogram is
/// still empty).
pub trait MetricsSnapshotsRender {
    fn render(&self, elapsed: Duration) -> String;
}

impl MetricsSnapshotsRender for [MetricsSnapshot] {
    fn render(&self, elapsed: Duration) -> String {
        let elapsed_secs = elapsed.as_secs();
        let max_committed = self
            .iter()
            .map(|s| s.total_committed_leaders())
            .max()
            .unwrap_or(0);
        let mut parts = vec![
            format!("[t={elapsed_secs}s]"),
            format!("committed={max_committed}"),
        ];
        let elapsed_secs_f = elapsed.as_secs_f64();
        if elapsed_secs_f > 0.0 && !self.is_empty() {
            let mean_committed = self
                .iter()
                .map(|s| s.total_committed_leaders())
                .sum::<u64>() as f64
                / self.len() as f64;
            if mean_committed > 0.0 {
                parts.push(format!("{:.1} commits/s", mean_committed / elapsed_secs_f));
            }
            let tx_counts: Vec<u64> = self
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
        let p50 = mean_per_replica(self, |s| s.latency_percentile_ms(0.5));
        let p90 = mean_per_replica(self, |s| s.latency_percentile_ms(0.9));
        if let (Some(p50), Some(p90)) = (p50, p90) {
            parts.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
        }
        parts.join(" · ")
    }
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
