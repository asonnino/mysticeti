// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Display extension traits for dag's run-result types. The domain types stay plain;
//! everything presentation-shaped lives here and co-locates the renderer's knowledge of
//! run results in one file. Adding a new run kind (e.g. testbed in #64) means another
//! `impl RunResultRender` alongside the sim one.

use dag::metrics::{Outcome, RunResult};
use simulator::SimulationConfig;

use super::table::{self, ReplicaRow};
use super::{GREEN, RED, RESET, YELLOW};

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

impl RunResultRender for RunResult<SimulationConfig> {
    fn render(&self, color: bool) -> String {
        let outcome = self.outcome;
        let commit_counts = self.leaders_committed_per_replica();

        let mut out = outcome.badge(color);
        out.push('\n');

        // Collapse to a one-line summary when every replica committed identically and
        // nothing else is noteworthy (sim-only pattern; testbed shutdowns aren't
        // synchronised so counts almost never align there).
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
