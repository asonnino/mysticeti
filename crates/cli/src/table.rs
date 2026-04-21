// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use dag::{
    authority::Authority,
    metrics::{
        BLOCK_SYNC_REQUESTS_SENT, LABEL_AUTHORITY, LABEL_WORKLOAD, LATENCY_S, LEADER_TIMEOUT_TOTAL,
        MetricsSnapshot,
    },
};
use simulator::{SimulationConfig, SimulationResults};
use tabled::{Table, Tabled, settings::Style};

use crate::commands::simulate::Outcome;

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

    fn new(parameter: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            parameter: parameter.into(),
            value: value.into(),
        }
    }
}

#[derive(Tabled)]
pub struct ValidatorRow {
    #[tabled(rename = "validator")]
    validator: usize,
    #[tabled(rename = "committed leaders")]
    committed_leaders: usize,
    #[tabled(rename = "commits/s")]
    commits_per_sec: String,
    #[tabled(rename = "mean latency")]
    mean_latency: String,
    #[tabled(rename = "leader timeouts")]
    leader_timeouts: u64,
    #[tabled(rename = "missing blocks")]
    pub missing_blocks: String,
    #[tabled(rename = "sync requests sent")]
    sync_requests_sent: u64,
}

impl ValidatorRow {
    pub fn for_results(results: &SimulationResults, duration_secs: u64) -> Vec<Self> {
        results
            .committed_leaders
            .iter()
            .zip(results.metrics.iter())
            .enumerate()
            .map(|(i, (leaders, metrics))| {
                Self::new(Authority::from(i), leaders.len(), duration_secs, metrics)
            })
            .collect()
    }

    fn new(
        authority: Authority,
        committed_leaders: usize,
        duration_secs: u64,
        metrics: &MetricsSnapshot,
    ) -> Self {
        let label = authority.to_string();
        let commits_per_sec = metrics
            .leader_commits_per_second(authority, Duration::from_secs(duration_secs))
            .map(|rate| format!("{rate:.1}"))
            .unwrap_or_else(|| "—".into());
        let mean_latency = metrics
            .histogram_mean(LATENCY_S, &[(LABEL_WORKLOAD, "shared")])
            .map(|seconds| format!("{:.0} ms", seconds * 1000.0))
            .unwrap_or_else(|| "—".into());
        let leader_timeouts = metrics.metric(LEADER_TIMEOUT_TOTAL, &[]) as u64;
        let missing_blocks = metrics.missing_blocks(authority);
        let sync_requests_sent =
            metrics.metric(BLOCK_SYNC_REQUESTS_SENT, &[(LABEL_AUTHORITY, &label)]) as u64;

        Self {
            validator: authority.index(),
            committed_leaders,
            commits_per_sec,
            mean_latency,
            leader_timeouts,
            missing_blocks: missing_blocks.to_string(),
            sync_requests_sent,
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
