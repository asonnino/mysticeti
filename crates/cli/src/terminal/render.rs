// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Display extension traits used by [`super::Terminal`]. The domain types in `dag`,
//! `simulator`, and `replica` stay plain; every presentation concern (config tables,
//! live progress lines, outcome badges, per-result blocks) lives here so the
//! renderer's knowledge is co-located in one file.

use std::time::Duration;

use dag::metrics::SnapshotAggregate;
use replica::result::{Outcome, RunResult};
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
        let aggregate = SnapshotAggregate::new(&self.metrics);
        let commit_counts = aggregate.committed_leaders_per_replica();

        let mut out = outcome.badge(color);
        out.push('\n');

        let uniform_commits = commit_counts
            .first()
            .map(|first| commit_counts.iter().all(|c| c == first))
            .unwrap_or(true);
        if outcome != Outcome::Diverged && uniform_commits {
            let committed = commit_counts.first().copied().unwrap_or_default();
            let rate = match aggregate.committed_leaders_per_second(self.duration) {
                Some(rate) => format!("{rate:.1} commits/s"),
                None => "— commits/s".into(),
            };
            let mut headline = Vec::new();
            if let (Some(p50), Some(p90)) = (
                aggregate.mean_latency_percentile_ms(0.5),
                aggregate.mean_latency_percentile_ms(0.9),
            ) {
                headline.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
            }
            if let Some(tps) = aggregate.transactions_per_second(self.duration) {
                headline.push(format!("{tps:.0} TPS"));
            }
            let headline = headline.join(" · ");
            if headline.is_empty() {
                out.push_str(&format!("  {committed} commits, {rate}"));
            } else {
                out.push_str(&format!("  {headline} ({committed} commits, {rate})"));
            }
        } else {
            out.push_str(&table::render(ReplicaRow::iter_for_result(self)));
        }
        out
    }
}

/// Live one-line render of an aggregate, e.g.
/// `[t=Ns] · committed=N · X.X commits/s · Y tx/s · p50 N ms · p90 N ms`.
/// Sub-fields are omitted when the underlying aggregator returns `None` (e.g. before
/// the load generator's `initial_delay` has elapsed and the latency histogram is
/// still empty).
pub trait AggregateRender {
    fn render(&self, elapsed: Duration) -> String;
}

impl AggregateRender for SnapshotAggregate<'_> {
    fn render(&self, elapsed: Duration) -> String {
        let mut parts = vec![
            format!("[t={}s]", elapsed.as_secs()),
            format!("committed={}", self.max_committed_leaders()),
        ];
        let elapsed_secs_f = elapsed.as_secs_f64();
        if elapsed_secs_f > 0.0 {
            if let Some(mean) = self.mean_committed_leaders() {
                parts.push(format!("{:.1} commits/s", mean / elapsed_secs_f));
            }
            if let Some(mean) = self.mean_committed_transactions() {
                parts.push(format!("{:.0} tx/s", mean / elapsed_secs_f));
            }
        }
        if let (Some(p50), Some(p90)) = (
            self.mean_latency_percentile_ms(0.5),
            self.mean_latency_percentile_ms(0.9),
        ) {
            parts.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
        }
        parts.join(" · ")
    }
}
