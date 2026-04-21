// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io::IsTerminal, time::Duration};

use eyre::{Result, eyre};
use indicatif::{ProgressBar, ProgressStyle};
use simulator::{SimulationConfig, SimulationResults, SimulationRunner};

use crate::{
    banner::BannerPrinter,
    commands::simulate::Outcome,
    table::{self, ConfigRow, SuiteRow, ValidatorRow},
};

pub const BLUE_FOREGROUND: &str = "\x1b[34m";
pub const BLUE_BACKGROUND: &str = "\x1b[44m";
pub const BOLD: &str = "\x1b[1m";
pub const DIM: &str = "\x1b[2m";
pub const GREEN: &str = "\x1b[32m";
pub const RED: &str = "\x1b[31m";
pub const YELLOW: &str = "\x1b[33m";
pub const RESET: &str = "\x1b[0m";

/// True when stderr points at an interactive terminal and
/// is therefore safe to emit ANSI escape codes to.
pub fn stderr_supports_color() -> bool {
    std::io::stderr().is_terminal()
}

/// Owns the terminal-capability flag and all presentation
/// methods for the `simulate` command. One instance per
/// invocation; methods borrow `&self`.
pub struct Reporter {
    color: bool,
}

impl Default for Reporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter {
    pub fn new() -> Self {
        Self {
            color: stderr_supports_color(),
        }
    }

    pub fn banner(&self, protocol: &str, info: &[(&str, &str)]) {
        BannerPrinter::new(protocol, info).print();
    }

    pub fn config_summary(&self, index: usize, total: usize, config: &SimulationConfig) {
        let heading = match (total > 1, config.name.as_deref()) {
            (true, Some(name)) => Some(format!("Simulation [{index}/{total}]: {name}")),
            (true, None) => Some(format!("Simulation [{index}/{total}]")),
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
    }

    /// Execute one simulation: spinner → run → badge → per-validator
    /// report. Returns the outcome and the suite-level row so the
    /// caller can track suite-wide aggregates.
    pub async fn run(&self, config: SimulationConfig) -> Result<(Outcome, SuiteRow)> {
        let run_name = config.name.clone().unwrap_or_else(|| "unnamed".into());
        let committee_size = config.committee_size;
        let duration_secs = config.duration_secs;

        let spinner = self.start_spinner();
        let results = tokio::task::spawn_blocking(move || SimulationRunner::new(config).run())
            .await
            .map_err(|error| eyre!("Simulation task panicked: {error}"))?;
        self.finish_spinner(spinner);

        let commit_counts: Vec<usize> = results.committed_leaders.iter().map(|v| v.len()).collect();
        let outcome = Outcome::from(results.commits_consistent, &commit_counts);
        self.render_run(&results, duration_secs, outcome);
        println!();

        let suite_row = SuiteRow::new(
            &run_name,
            committee_size,
            duration_secs,
            outcome,
            &commit_counts,
        );
        Ok((outcome, suite_row))
    }

    fn render_run(&self, results: &SimulationResults, duration_secs: u64, outcome: Outcome) {
        self.run_badge(outcome);

        let rows = ValidatorRow::for_results(results, duration_secs);

        // Collapse to a single-line summary in the happy path when every
        // validator committed the same leaders and nothing is noteworthy.
        let commit_counts: Vec<usize> = results.committed_leaders.iter().map(|v| v.len()).collect();
        let uniform_commits = commit_counts
            .first()
            .map(|first| commit_counts.iter().all(|c| c == first))
            .unwrap_or(true);
        let no_missing = rows.iter().all(|row| row.missing_blocks == "0");

        if outcome != Outcome::Diverged && uniform_commits && no_missing {
            let validators = rows.len();
            let committed = commit_counts.first().copied().unwrap_or_default();
            let rate = if duration_secs == 0 {
                "— commits/s".into()
            } else {
                format!("{:.1} commits/s", committed as f64 / duration_secs as f64)
            };
            self.run_summary_line(validators, committed, &rate);
            return;
        }

        self.validators_table(rows);
    }

    fn start_spinner(&self) -> Option<ProgressBar> {
        if !self.color {
            return None;
        }
        println!();
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::with_template(" {spinner} Simulating… {elapsed}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        spinner.enable_steady_tick(Duration::from_millis(100));
        Some(spinner)
    }

    fn finish_spinner(&self, spinner: Option<ProgressBar>) {
        if let Some(spinner) = spinner {
            spinner.finish_and_clear();
        }
    }

    fn run_badge(&self, outcome: Outcome) {
        if self.color {
            println!(
                "{color}{glyph} {message}{RESET}",
                color = outcome.color(),
                glyph = outcome.glyph(),
                message = outcome.message(),
            );
        } else {
            println!("{outcome}");
        }
    }

    fn run_summary_line(&self, validators: usize, committed: usize, rate: &str) {
        println!("  {validators} validators · {committed} committed leaders ({rate})");
    }

    fn validators_table(&self, rows: Vec<ValidatorRow>) {
        println!("{}", table::render(rows));
    }

    pub fn suite_summary(&self, rows: &[SuiteRow]) {
        println!();
        if self.color {
            println!("{BOLD}Suite summary{RESET}");
        } else {
            println!("Suite summary");
        }
        println!("{}", table::render(rows.iter().cloned()));
    }
}
