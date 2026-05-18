// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Terminal renderer for simulation / testbed results. All ANSI-aware and
//! tabled-driven display code lives here.

pub mod banner;
pub mod progress;
mod render;
pub mod table;

use std::{io::IsTerminal, time::Duration};

use dag::metrics::SnapshotAggregate;
use replica::result::RunResult;

pub(crate) use self::render::{AggregateRender, ConfigRender, RunResultRender};
use self::table::SuiteRow;

pub use self::banner::BannerPrinter;
pub use self::progress::Progress;

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
/// long-lived progress indicator that the print methods toggle on and off.
pub struct Terminal {
    color: bool,
    total: usize,
    suite_rows: Vec<SuiteRow>,
    progress: Progress,
}

impl Terminal {
    pub(crate) fn new(total: usize) -> Self {
        let color = stderr_supports_color();
        Self {
            color,
            total,
            suite_rows: Vec::with_capacity(total),
            progress: Progress::new(color),
        }
    }

    /// Print the per-run heading and config table. The heading is omitted when
    /// there is only one run with no config name; the table is omitted when
    /// [`ConfigRender::config_rows`] is empty. Callers start the progress
    /// indicator separately via [`Self::start_progress_animation`].
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
            println!("{}", config.render(self.color));
        }
    }

    /// Advance the bar and optionally print the live progress line on stderr.
    pub(crate) fn print_status(
        &mut self,
        elapsed: Duration,
        aggregate: Option<&SnapshotAggregate<'_>>,
    ) {
        if let Some(aggregate) = aggregate {
            self.progress
                .suspend(|| eprintln!("{}", aggregate.render(elapsed)));
        }
        self.progress.set_elapsed(elapsed);
    }

    /// Stop the progress indicator, print the per-result block (badge + per-replica
    /// table or happy-path headline), and record the suite row for later aggregate
    /// display.
    pub(crate) fn print_results<C: ConfigRender>(&mut self, result: &RunResult<C>) {
        self.stop_progress_animation();
        println!("{}", result.render(self.color));
        println!();

        let run_name = result
            .config
            .name()
            .map(str::to_owned)
            .unwrap_or_else(|| "unnamed".into());
        let commit_counts = SnapshotAggregate::new(&result.metrics).committed_leaders_per_replica();
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

    /// Switch the progress indicator to an indeterminate spinner with a custom
    /// label. Useful for follow-on phases (e.g. result collection) that come
    /// after a determinate run and have no known duration.
    pub(crate) fn start_progress_animation(&mut self, duration: Option<Duration>, label: &str) {
        self.progress.start(duration, label);
    }

    /// Tear down the active progress indicator and clear its line. Use this
    /// before any plain `eprintln!` calls so the bar doesn't fight for the
    /// line with the printed text.
    pub(crate) fn stop_progress_animation(&mut self) {
        self.progress.stop();
    }
}
