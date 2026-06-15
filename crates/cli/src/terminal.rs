// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Terminal renderer for simulation / testbed results. All ANSI-aware and
//! tabled-driven display code lives here.

pub mod banner;
pub mod progress;
mod render;
pub mod table;

use std::{future::Future, io::IsTerminal, time::Duration};

pub(crate) use self::render::{ConfigRender, OutcomeDisplay, ResultRender, StatusRender};
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
        let kind = config.run_kind();
        let heading = match (self.total > 1, config.heading()) {
            (true, Some(name)) => Some(format!(
                "{kind} [{index}/{total}]: {name}",
                total = self.total
            )),
            (true, None) => Some(format!("{kind} [{index}/{total}]", total = self.total)),
            (false, Some(name)) => Some(format!("{kind}: {name}")),
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

    /// Print a titled key/value block in the banner's dim-key / bold-value style,
    /// preceded by a blank line so it separates cleanly from any progress line above.
    pub(crate) fn print_details(&self, title: &str, rows: &[(&str, &str)]) {
        if self.color {
            println!("\n{BOLD}{title}{RESET}");
            for (key, value) in rows {
                println!("  {DIM}{key}:{RESET} {BOLD}{value}{RESET}");
            }
        } else {
            println!("\n{title}");
            for (key, value) in rows {
                println!("  {key}: {value}");
            }
        }
    }

    /// Print the live heartbeat line on stderr and advance the bar to `elapsed`.
    /// For an advance without a line (sub-heartbeat ticks), use [`Self::set_elapsed`].
    pub(crate) fn print_status(&mut self, elapsed: Duration, status: &impl StatusRender) {
        self.progress
            .suspend(|| eprintln!("{}", status.render_status_line(elapsed)));
        self.progress.set_elapsed(elapsed);
    }

    /// Stop the progress indicator, print the per-result block (badge + per-replica
    /// table or happy-path headline), and record the suite row for later aggregate
    /// display. Config and result are separate: the result owns the metrics/outcome
    /// while the config supplies the run's name and committee size.
    pub(crate) fn print_results(&mut self, config: &impl ConfigRender, result: &impl ResultRender) {
        self.stop_progress_animation();
        let block = result.render_block(self.color);
        if !block.is_empty() {
            println!("{block}");
            println!();
        }

        let run_name = config.name().unwrap_or_else(|| "unnamed".into());
        if let Some(row) = result.suite_row(&run_name, config.committee_size()) {
            self.suite_rows.push(row);
        }
    }

    /// Print the suite summary when more than one run recorded a row; no-op for
    /// single-run commands and when no run produced a row (e.g. remote benchmarks
    /// with log analysis disabled), so we never print an empty table.
    pub(crate) fn print_summary(&self) {
        if self.total <= 1 || self.suite_rows.is_empty() {
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

    /// Advance the determinate bar to `elapsed`. No-op for the spinner / idle.
    pub(crate) fn set_elapsed(&self, elapsed: Duration) {
        self.progress.set_elapsed(elapsed);
    }

    /// Run `work` under an indeterminate spinner labelled `label`, clearing it
    /// when the future resolves. Delegates to [`Progress::track`].
    pub(crate) async fn track<T, E, F>(&mut self, label: &str, work: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        self.progress.track(label, work).await
    }

    /// Run `f` while the active indicator is suspended (cleared, then redrawn),
    /// so plain prints don't fight the bar for the line.
    pub(crate) fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        self.progress.suspend(f)
    }
}
