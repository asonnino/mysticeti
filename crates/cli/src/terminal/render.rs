// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Display extension traits used by [`super::Terminal`]. The domain types in `dag`,
//! `simulator`, and `replica` stay plain; every presentation concern (config tables,
//! live progress lines, outcome badges, per-result blocks) lives here so the
//! renderer's knowledge is co-located in one file.

use std::time::Duration;

use dag::{authority::Authority, metrics::SnapshotAggregate};
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
            ConfigRow::new("Protocol", self.replica_parameters.consensus.to_string()),
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
        // Configs are already printed in the banner.
        vec![]
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
            out.push_str(&aggregate.render(self.duration));
        } else {
            let rows = self
                .metrics
                .iter()
                .enumerate()
                .map(move |(index, metrics)| {
                    ReplicaRow::new(Authority::from(index), self.duration, metrics)
                });
            out.push_str(&table::render(rows));
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
            format!("[t={}s]", fixed_length_format(elapsed.as_secs() as f64)),
            format!(
                "committed={}",
                fixed_length_format(self.max_committed_leaders() as f64),
            ),
        ];
        let elapsed_secs_f = elapsed.as_secs_f64();
        if elapsed_secs_f > 0.0 {
            if let Some(mean) = self.mean_committed_leaders() {
                parts.push(format!(
                    "commits/s={}",
                    fixed_length_format(mean / elapsed_secs_f),
                ));
            }
            if let Some(mean) = self.mean_committed_transactions() {
                parts.push(format!(
                    "tx/s={}",
                    fixed_length_format(mean / elapsed_secs_f),
                ));
            }
        }
        if let Some(p50) = self.mean_latency_percentile_ms(0.5) {
            parts.push(format!("p50={}ms", fixed_length_format(p50)));
        }
        if let Some(p90) = self.mean_latency_percentile_ms(0.9) {
            parts.push(format!("p90={}ms", fixed_length_format(p90)));
        }
        parts.join(" · ")
    }
}

/// Format a non-negative number into a 4-character right-aligned string,
/// using scale suffixes when the value reaches 1000:
///
/// * `0..1000`  → bare integer, space-padded (`"   9"`, `" 999"`).
/// * `1k..1M`   → integer thousands + `k` (`"  1k"`, `"190k"`).
/// * up to `T` (`10^12`) for the suffix sequence `["k", "M", "B", "T"]`
///   (`B` for billion, not SI's `G`).
///
/// Used by live one-liner fields so they stay aligned across heartbeat
/// refreshes regardless of magnitude.
fn fixed_length_format(value: f64) -> String {
    const UNITS: &[&str] = &["", "k", "M", "B", "T"];
    let mut scaled = value;
    let mut unit = 0;
    // Compare the rounded value against the 1000 threshold so values like
    // 999.5 escalate to the next unit (1k, formatted as "1k") instead of
    // rounding to "1000" and overflowing the 4-char field.
    while scaled.round() >= 1000.0 && unit < UNITS.len() - 1 {
        scaled /= 1000.0;
        unit += 1;
    }
    let formatted = format!("{}{}", scaled.round() as u64, UNITS[unit]);
    format!("{formatted:>4}")
}
