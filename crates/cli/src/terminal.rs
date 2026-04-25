// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Terminal renderer for simulation / testbed results. All ANSI-aware and
//! tabled-driven display code lives here.

pub mod banner;
mod result;
mod spinner;
pub mod table;

use std::io::IsTerminal;

use dag::metrics::RunResult;
use simulator::SimulationConfig;

use self::result::RunResultRender;
use self::spinner::Spinner;
use self::table::{ConfigRow, SuiteRow};

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

    /// Begin a run: print the per-run heading + config table and arm the spinner.
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
        println!("{}", result.render(self.color));
        println!();

        let run_name = result
            .config
            .name
            .clone()
            .unwrap_or_else(|| "unnamed".into());
        let commit_counts = result.leaders_committed_per_replica();
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
}
