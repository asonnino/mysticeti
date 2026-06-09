// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Display extension traits used by [`super::Terminal`]. The domain types in `dag`,
//! `simulator`, and `replica` stay plain; every presentation concern (config tables,
//! live progress lines, outcome badges, per-result blocks) lives here so the
//! renderer's knowledge is co-located in one file.

use std::time::Duration;

use dag::{authority::Authority, metrics::SnapshotAggregate};
use orchestrator::benchmark::BenchmarkParameters;
use orchestrator::collector::LiveStats;
use replica::result::{Outcome, RunResult};
use replica::testbed::TestbedConfig;
use simulator::SimulationConfig;

use std::fmt::{self, Write as _};

use super::table::{self, ReplicaRow, SuiteRow};
use super::{BOLD, DIM, GREEN, RED, RESET, YELLOW};

/// Adapter trait letting [`super::Terminal`] render any run config: the per-run
/// heading uses [`Self::name`], the suite summary's `nodes` column uses
/// [`Self::committee_size`], and the per-run banner-style key/value list
/// renders [`Self::config_rows`] (empty means "no config to print for this
/// command").
pub trait ConfigRender {
    /// Per-run name shown in the heading and the suite summary's `name` column.
    fn name(&self) -> Option<String>;
    fn committee_size(&self) -> usize;
    fn config_rows(&self) -> Vec<(&'static str, String)>;

    /// Noun used in the per-run heading (`"{kind} [i/N]: {name}"`).
    fn run_kind(&self) -> &str;

    /// Render `config_rows` in the banner's border-less key/value style.
    fn render(&self, color: bool) -> String {
        let mut out = String::new();
        for (key, value) in self.config_rows() {
            if color {
                let _ = writeln!(out, "{DIM}{key}:{RESET} {BOLD}{value}{RESET}");
            } else {
                let _ = writeln!(out, "{key}: {value}");
            }
        }
        out
    }
}

impl ConfigRender for SimulationConfig {
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn run_kind(&self) -> &str {
        "Simulation"
    }

    fn committee_size(&self) -> usize {
        self.committee_size
    }

    fn config_rows(&self) -> Vec<(&'static str, String)> {
        vec![
            ("Protocol", self.replica_parameters.consensus.to_string()),
            ("Replicas", self.committee_size.to_string()),
            ("Topology", self.topology.to_string()),
            ("Duration", format!("{}s", self.duration_secs)),
            (
                "Latency range",
                format!("{}-{} ms", self.latency_min_ms, self.latency_max_ms),
            ),
            ("RNG seed", self.rng_seed.to_string()),
        ]
    }
}

impl ConfigRender for TestbedConfig {
    fn name(&self) -> Option<String> {
        None
    }

    fn run_kind(&self) -> &str {
        "Testbed"
    }

    fn committee_size(&self) -> usize {
        self.committee_size
    }

    fn config_rows(&self) -> Vec<(&'static str, String)> {
        // Configs are already printed in the banner.
        vec![]
    }
}

impl<N: fmt::Display, C> ConfigRender for BenchmarkParameters<N, C> {
    fn name(&self) -> Option<String> {
        // The suite-summary `name` column needs a per-run identity; benchmarks
        // carry theirs in the `Display` impl (`"N nodes (faults) - L tx/s"`).
        Some(self.to_string())
    }

    fn run_kind(&self) -> &str {
        "Benchmark"
    }

    fn committee_size(&self) -> usize {
        self.nodes
    }

    fn config_rows(&self) -> Vec<(&'static str, String)> {
        // Committee size and commit are already printed in the banner; surface the
        // per-benchmark knobs (load, fault schedule, node parameters) that vary
        // across the suite.
        vec![
            ("Load", format!("{} tx/s", self.load)),
            ("Faults", self.settings.faults.to_string()),
            ("Node parameters", self.node_parameters.to_string()),
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

/// Live heartbeat renderer, driven by [`super::Terminal::print_status`] on each
/// progress tick. Implemented by the transient live-metrics views — the local
/// per-replica aggregate and the remote Prometheus-scraped stats — not by a
/// finished run (no result/verdict exists mid-run).
pub trait StatusRender {
    /// Heartbeat one-liner, e.g.
    /// `[t=Ns] · committed=N · X.X commits/s · Y tx/s · p50 N ms · p90 N ms`.
    /// Sub-fields are omitted when the underlying aggregator returns `None` (e.g.
    /// before the load generator's `initial_delay` has elapsed and the latency
    /// histogram is still empty).
    fn render_status_line(&self, elapsed: Duration) -> String;
}

impl StatusRender for SnapshotAggregate<'_> {
    fn render_status_line(&self, elapsed: Duration) -> String {
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

impl StatusRender for LiveStats {
    fn render_status_line(&self, elapsed: Duration) -> String {
        // Keys match the metric names `ReplicaProtocol` registers and the
        // collector's `{name}.p{quantile}` convention for histogram quantiles.
        // `latency_s_count` is the per-second rate of latency observations — one
        // per committed transaction — i.e. throughput; quantiles are in seconds.
        let mut parts = vec![format!(
            "[t={}s]",
            fixed_length_format(elapsed.as_secs() as f64)
        )];
        if let Some(tps) = self.get("latency_s_count") {
            parts.push(format!("tx/s={}", fixed_length_format(tps)));
        }
        if let Some(p50) = self.get("latency_s.p50") {
            parts.push(format!("p50={}ms", fixed_length_format(p50 * 1000.0)));
        }
        if let Some(p90) = self.get("latency_s.p90") {
            parts.push(format!("p90={}ms", fixed_length_format(p90 * 1000.0)));
        }
        parts.join(" · ")
    }
}

/// Final-result renderer, driven by [`super::Terminal::print_results`] once a run
/// finishes. Implemented by the end-of-run result types (`RunResult`, the remote
/// benchmark result).
pub trait ResultRender {
    /// Per-result block: outcome badge followed by a happy-path one-liner or the
    /// full per-replica table. Empty string means "nothing to print".
    fn render_block(&self, color: bool) -> String;

    /// One row for the suite summary table, given the per-run `name` and committee
    /// size resolved from the config. `None` contributes no row — used by results
    /// whose outcome could not be determined.
    fn suite_row(&self, name: &str, nodes: usize) -> Option<SuiteRow>;
}

impl<C> ResultRender for RunResult<C> {
    fn render_block(&self, color: bool) -> String {
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
            out.push_str(&aggregate.render_status_line(self.duration));
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

    fn suite_row(&self, name: &str, nodes: usize) -> Option<SuiteRow> {
        let commit_counts = SnapshotAggregate::new(&self.metrics).committed_leaders_per_replica();
        Some(SuiteRow::new(
            name,
            nodes,
            self.duration.as_secs(),
            self.outcome,
            &commit_counts,
        ))
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
