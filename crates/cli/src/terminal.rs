// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Terminal renderer for simulation / testbed results. All ANSI-aware and
//! tabled-driven display code lives here.

pub mod banner;
mod spinner;
pub mod table;

use std::{io::IsTerminal, time::Duration};

use dag::metrics::{AggregateMetrics, Outcome, RunResult};
use simulator::SimulationConfig;

use self::spinner::Spinner;
use self::table::{ConfigRow, ReplicaRow, SuiteRow};

pub use self::banner::BannerPrinter;

pub const BLUE_FOREGROUND: &str = "\x1b[34m";
pub const BLUE_BACKGROUND: &str = "\x1b[44m";
pub const BOLD: &str = "\x1b[1m";
pub const DIM: &str = "\x1b[2m";
pub const GREEN: &str = "\x1b[32m";
pub const RED: &str = "\x1b[31m";
pub const YELLOW: &str = "\x1b[33m";
pub const RESET: &str = "\x1b[0m";

/// True when stderr points at an interactive terminal and is therefore safe to emit ANSI
/// escape codes to.
pub fn stderr_supports_color() -> bool {
    std::io::stderr().is_terminal()
}

/// Display helpers for the plain [`Outcome`] enum. Extension trait on the domain type so
/// the domain stays free of presentation concerns.
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

/// Stateful renderer for one command invocation. Owns the suite-row history and the
/// active spinner across the start/stop bracket of each simulation.
pub struct Terminal {
    color: bool,
    total: usize,
    suite_rows: Vec<SuiteRow>,
    spinner: Option<Spinner>,
}

impl Terminal {
    pub fn new(total: usize) -> Self {
        Self {
            color: stderr_supports_color(),
            total,
            suite_rows: Vec::with_capacity(total),
            spinner: None,
        }
    }

    /// Begin a run: print the per-sim heading + config table and arm the spinner.
    pub fn start_run(&mut self, index: usize, config: &SimulationConfig) {
        let heading = match (self.total > 1, config.name.as_deref()) {
            (true, Some(name)) => Some(format!(
                "Simulation [{index}/{total}]: {name}",
                total = self.total
            )),
            (true, None) => Some(format!("Simulation [{index}/{total}]", total = self.total)),
            (false, Some(name)) => Some(format!("Simulation: {name}")),
            (false, None) => None,
        };

        println!();
        if let Some(heading) = heading {
            if self.color {
                println!("{BOLD}{heading}{RESET}");
            } else {
                println!("{heading}");
            }
        }
        println!("{}", table::render(ConfigRow::for_config(config)));

        self.spinner = Some(Spinner::new(self.color));
    }

    /// End a run: stop the spinner, render the per-replica output, and record the
    /// suite row for later aggregate display.
    pub fn stop_run(&mut self, result: &RunResult<SimulationConfig>) {
        if let Some(spinner) = self.spinner.take() {
            spinner.finish();
        }
        self.render_result(result);
        println!();

        let run_name = result
            .config
            .name
            .clone()
            .unwrap_or_else(|| "unnamed".into());
        let commit_counts = result.commit_count_per_replica();
        self.suite_rows.push(SuiteRow::new(
            &run_name,
            result.config.committee_size,
            result.config.duration_secs,
            result.outcome,
            &commit_counts,
        ));
    }

    /// End the command: print the suite summary when more than one simulation ran.
    pub fn finish(&self) {
        if self.total <= 1 {
            return;
        }
        println!();
        if self.color {
            println!("{BOLD}Suite summary{RESET}");
        } else {
            println!("Suite summary");
        }
        println!("{}", table::render(self.suite_rows.iter().cloned()));
    }

    fn render_result(&self, result: &RunResult<SimulationConfig>) {
        let outcome = result.outcome;
        let duration_secs = result.config.duration_secs;
        let commit_counts = result.commit_count_per_replica();

        println!("{}", outcome.badge(self.color));

        let rows = ReplicaRow::for_result(result, duration_secs);

        // Collapse to a single-line summary when every replica committed identically and
        // nothing else is noteworthy (sim-only pattern; testbed shutdowns aren't
        // synchronised so counts almost never align there).
        let uniform_commits = commit_counts
            .first()
            .map(|first| commit_counts.iter().all(|c| c == first))
            .unwrap_or(true);
        let aggregate = AggregateMetrics::new(&result.metrics);
        if outcome != Outcome::Diverged && uniform_commits && !aggregate.any_missing_blocks() {
            let duration = Duration::from_secs(duration_secs);
            let committed = commit_counts.first().copied().unwrap_or_default();
            let rate = match aggregate.leader_commits_per_second(duration) {
                Some(r) => format!("{r:.1} commits/s"),
                None => "— commits/s".into(),
            };
            let mut headline = Vec::new();
            if let (Some(p50), Some(p90)) = (aggregate.p50_latency_ms(), aggregate.p90_latency_ms())
            {
                headline.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
            }
            if let Some(tps) = aggregate.committed_tps(duration) {
                headline.push(format!("{tps:.0} TPS"));
            }
            print_summary_line(&headline.join(" · "), committed, &rate);
            return;
        }

        println!("{}", table::render(rows));
    }
}

fn print_summary_line(headline: &str, committed: usize, rate: &str) {
    if headline.is_empty() {
        println!("  {committed} commits, {rate}");
    } else {
        println!("  {headline} ({committed} commits, {rate})");
    }
}
