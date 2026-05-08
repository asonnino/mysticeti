// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Terminal renderer for simulation / testbed results. All ANSI-aware and
//! tabled-driven display code lives here.

pub mod banner;
mod render;
mod spinner;
pub mod table;

use std::{io::IsTerminal, time::Duration};

use dag::metrics::{MetricsSnapshot, RunResult};

pub(crate) use self::render::{ConfigRender, MetricsSnapshotsRender, RunResultRender};
use self::spinner::Spinner;
use self::table::SuiteRow;

pub use self::banner::BannerPrinter;

// ANSI SGR escapes. `BLUE_FOREGROUND` and `BLUE_BACKGROUND` pair up in the banner art:
// one paints character glyphs, the other fills the cell behind them.
pub const BLUE_FOREGROUND: &str = "\x1b[34m"; // banner art edges (half-block glyphs)
pub const BLUE_BACKGROUND: &str = "\x1b[44m"; // banner art fill (full-block glyph)
pub const GREEN: &str = "\x1b[32m"; // Outcome::Pass badge
pub const YELLOW: &str = "\x1b[33m"; // Outcome::NoProgress badge
pub const RED: &str = "\x1b[31m"; // Outcome::Diverged badge
pub const BOLD: &str = "\x1b[1m"; // headings, protocol line, banner key values
pub const DIM: &str = "\x1b[2m"; // banner info-key labels
pub const RESET: &str = "\x1b[0m";

/// True when stderr points at an interactive terminal and is therefore safe to emit ANSI
/// escape codes to.
pub fn stderr_supports_color() -> bool {
    std::io::stderr().is_terminal()
}

/// Stateful renderer for one command invocation. Owns the suite-row history and a
/// long-lived spinner that the print methods toggle on and off.
pub struct Terminal {
    color: bool,
    total: usize,
    suite_rows: Vec<SuiteRow>,
    spinner: Spinner,
}

impl Terminal {
    pub(crate) fn new(total: usize) -> Self {
        let color = stderr_supports_color();
        Self {
            color,
            total,
            suite_rows: Vec::with_capacity(total),
            spinner: Spinner::new(color),
        }
    }

    /// Print the per-run heading and config table, then start the spinner.
    /// The heading is omitted when there is only one run with no config name;
    /// the table is omitted when [`ConfigRender::config_rows`] is empty.
    pub(crate) fn print_config<C: ConfigRender>(&mut self, index: usize, config: &C) {
        let heading = match (self.total > 1, config.name()) {
            (true, Some(name)) => Some(format!(
                "Simulation [{index}/{total}]: {name}",
                total = self.total
            )),
            (true, None) => Some(format!("Simulation [{index}/{total}]", total = self.total)),
            (false, Some(name)) => Some(format!("Simulation: {name}")),
            (false, None) => None,
        };

        let rows = config.config_rows();
        if heading.is_some() || !rows.is_empty() {
            println!();
        }
        if let Some(heading) = heading {
            if self.color {
                println!("{BOLD}{heading}{RESET}");
            } else {
                println!("{heading}");
            }
        }
        if !rows.is_empty() {
            println!("{}", table::render(rows));
        }

        self.spinner.start();
    }

    /// Print the live progress line on stderr without disturbing the spinner —
    /// the bar is suspended around the write, so its elapsed timer stays
    /// monotonic across heartbeats.
    pub(crate) fn print_status(&mut self, elapsed: Duration, snapshots: &[MetricsSnapshot]) {
        self.spinner
            .suspend(|| eprintln!("{}", snapshots.render(elapsed)));
    }

    /// Stop the spinner, print the per-result block (badge + per-replica table or
    /// happy-path headline), and record the suite row for later aggregate display.
    pub(crate) fn print_results<C: ConfigRender>(&mut self, result: &RunResult<C>) {
        self.spinner.stop();
        println!("{}", result.render(self.color));
        println!();

        let run_name = result
            .config
            .name()
            .map(str::to_owned)
            .unwrap_or_else(|| "unnamed".into());
        let commit_counts = result.leaders_committed_per_replica();
        self.suite_rows.push(SuiteRow::new(
            &run_name,
            result.config.committee_size(),
            result.duration.as_secs(),
            result.outcome,
            &commit_counts,
        ));
    }

    /// Print the suite summary when more than one run was recorded; no-op for
    /// single-run commands.
    pub(crate) fn print_summary(&self) {
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
}
