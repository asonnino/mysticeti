// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::{
    authority::Authority,
    metrics::{MetricsSnapshot, Outcome, RunResult},
};
use simulator::SimulationConfig;
use tabled::{Table, Tabled, settings::Style};

use super::result::OutcomeDisplay;

/// Render any iterable of `Tabled` rows with the suite's
/// standard rounded style. Single call site so the border
/// style can change in one place.
pub fn render<T: Tabled>(rows: impl IntoIterator<Item = T>) -> String {
    Table::new(rows).with(Style::rounded()).to_string()
}

#[derive(Tabled)]
pub struct ConfigRow {
    #[tabled(rename = "parameter")]
    parameter: String,
    #[tabled(rename = "value")]
    value: String,
}

impl ConfigRow {
    fn new(parameter: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            parameter: parameter.into(),
            value: value.into(),
        }
    }

    pub fn for_config(config: &SimulationConfig) -> Vec<Self> {
        vec![
            Self::new("Committee size", config.committee_size.to_string()),
            Self::new("Topology", config.topology.to_string()),
            Self::new("Duration", format!("{}s", config.duration_secs)),
            Self::new(
                "Latency range",
                format!("{}-{} ms", config.latency_min_ms, config.latency_max_ms),
            ),
            Self::new("RNG seed", config.rng_seed.to_string()),
        ]
    }
}

#[derive(Tabled)]
pub struct ReplicaRow {
    #[tabled(rename = "replica")]
    replica: Authority,
    #[tabled(rename = "committed leaders")]
    committed_leaders: usize,
    #[tabled(rename = "commits/s")]
    commits_per_sec: String,
    #[tabled(rename = "p50 latency", display_with = "fmt_latency_ms")]
    p50_latency_ms: Option<f64>,
    #[tabled(rename = "p90 latency", display_with = "fmt_latency_ms")]
    p90_latency_ms: Option<f64>,
    #[tabled(rename = "leader timeouts")]
    leader_timeouts: u64,
}

impl ReplicaRow {
    fn new(
        authority: Authority,
        committed_leaders: usize,
        duration_secs: u64,
        metrics: &MetricsSnapshot,
    ) -> Self {
        // Per-replica rate pairs with the `committed_leaders` cell on the same row (both are
        // this replica's view of the committed chain): divide the row's own chain length by the
        // run duration rather than going back through a metric.
        let commits_per_sec = if duration_secs == 0 {
            "—".into()
        } else {
            format!("{:.1}", committed_leaders as f64 / duration_secs as f64)
        };
        Self {
            replica: authority,
            committed_leaders,
            commits_per_sec,
            p50_latency_ms: metrics.latency_percentile_ms(0.5),
            p90_latency_ms: metrics.latency_percentile_ms(0.9),
            leader_timeouts: metrics.leader_timeouts(),
        }
    }

    pub fn for_result<C>(result: &RunResult<C>) -> Vec<Self> {
        let duration_secs = result.duration.as_secs();
        result
            .metrics
            .iter()
            .zip(result.leaders_committed_per_replica())
            .enumerate()
            .map(|(index, (metrics, committed_leaders))| {
                Self::new(
                    Authority::from(index),
                    committed_leaders,
                    duration_secs,
                    metrics,
                )
            })
            .collect()
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
