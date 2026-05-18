// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use dag::{authority::Authority, metrics::MetricsSnapshot};
use replica::result::Outcome;
use tabled::{Table, Tabled, settings::Style};

use super::render::OutcomeDisplay;

/// Render any iterable of `Tabled` rows with the suite's standard rounded style.
pub fn render<T: Tabled>(rows: impl IntoIterator<Item = T>) -> String {
    Table::new(rows).with(Style::rounded()).to_string()
}

#[derive(Tabled)]
pub struct ReplicaRow {
    #[tabled(rename = "replica")]
    replica: Authority,
    #[tabled(rename = "committed leaders")]
    committed_leaders: u64,
    #[tabled(rename = "commits/s")]
    committed_leaders_per_second: String,
    #[tabled(rename = "tx/s")]
    tx_per_second: String,
    #[tabled(rename = "p50 latency", display = "fmt_latency_ms")]
    p50_latency_ms: Option<f64>,
    #[tabled(rename = "p90 latency", display = "fmt_latency_ms")]
    p90_latency_ms: Option<f64>,
    #[tabled(rename = "timeouts")]
    leader_timeouts: u64,
}

impl ReplicaRow {
    pub(super) fn new(authority: Authority, duration: Duration, metrics: &MetricsSnapshot) -> Self {
        let committed_leaders = metrics.total_committed_leaders();
        let committed_leaders_per_second =
            if let Some(rate) = metrics.committed_leaders_per_second(duration) {
                format!("{rate:.1}")
            } else {
                "—".into()
            };
        let tx_per_second = match metrics.transactions_per_second(duration) {
            Some(rate) => format!("{rate:.0}"),
            None => "—".into(),
        };
        Self {
            replica: authority,
            committed_leaders,
            committed_leaders_per_second,
            tx_per_second,
            p50_latency_ms: metrics.latency_percentile_ms(0.5),
            p90_latency_ms: metrics.latency_percentile_ms(0.9),
            leader_timeouts: metrics.leader_timeouts(),
        }
    }
}

#[derive(Tabled, Clone)]
pub struct SuiteRow {
    #[tabled(rename = "name")]
    name: String,
    #[tabled(rename = "nodes")]
    nodes: usize,
    #[tabled(rename = "duration")]
    duration: String,
    #[tabled(rename = "consistency")]
    consistency: String,
    #[tabled(rename = "committed leaders")]
    committed_leaders: String,
}

impl SuiteRow {
    pub fn new(
        name: &str,
        nodes: usize,
        duration_secs: u64,
        outcome: Outcome,
        commit_counts: &[usize],
    ) -> Self {
        // Plain glyphs inside the table: tabled 0.12 sizes columns by
        // raw byte length so ANSI colour escapes would skew alignment.
        let consistency = outcome.glyph().into();
        let committed_leaders = match (commit_counts.iter().min(), commit_counts.iter().max()) {
            (Some(min), Some(max)) if min == max => format!("{min}"),
            (Some(min), Some(max)) => format!("{min}–{max}"),
            _ => "—".into(),
        };
        Self {
            name: name.into(),
            nodes,
            duration: format!("{duration_secs}s"),
            consistency,
            committed_leaders,
        }
    }
}

fn fmt_latency_ms(value: &Option<f64>) -> String {
    match value {
        Some(ms) => format!("{ms:.0} ms"),
        None => "—".into(),
    }
}
