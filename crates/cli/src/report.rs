// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use dag::{authority::Authority, metrics::ReplicaStats};
use eyre::{Result, bail};
use serde::Serialize;
use simulator::{SimulationConfig, SimulationResults};

use crate::commands::simulate::Outcome;

#[derive(Serialize)]
pub struct SimulationReport {
    pub config: SimulationConfig,
    pub outcome: Outcome,
    pub commits_consistent: bool,
    pub duration_secs: u64,
    pub replicas: Vec<ReplicaReport>,
}

#[derive(Serialize)]
pub struct ReplicaReport {
    pub authority: Authority,
    /// Each leader formatted via `BlockReference::Display` (`"A3"` — authority+round).
    /// `BlockDigest::serialize` uses `serialize_bytes` (bincode wire format) which
    /// `serde_yaml` can't encode; stringifying in the report layer keeps the wire format
    /// untouched.
    pub committed_leaders: Vec<String>,
    pub commits: usize,
    pub commits_per_sec: Option<f64>,
    #[serde(flatten)]
    pub stats: ReplicaStats,
    /// Full per-replica metrics in the Prometheus text exposition format — every counter,
    /// gauge, and histogram bucket the run emitted. Parseable by `promtool`, Prometheus, and
    /// most TSDB ingesters.
    pub metrics: String,
}

impl SimulationReport {
    pub fn new(config: SimulationConfig, results: &SimulationResults, outcome: Outcome) -> Self {
        let duration_secs = config.duration_secs;
        let replicas = results
            .committed_leaders
            .iter()
            .zip(results.metrics.iter())
            .enumerate()
            .map(|(index, (leaders, metrics))| {
                let authority = Authority::from(index);
                let commits = leaders.len();
                let commits_per_sec =
                    (duration_secs > 0).then(|| commits as f64 / duration_secs as f64);
                ReplicaReport {
                    authority,
                    committed_leaders: leaders.iter().map(|leader| leader.to_string()).collect(),
                    commits,
                    commits_per_sec,
                    stats: metrics.replica_stats(authority),
                    metrics: metrics.to_prometheus_text(),
                }
            })
            .collect();
        Self {
            config,
            outcome,
            commits_consistent: results.commits_consistent,
            duration_secs,
            replicas,
        }
    }
}

enum Format {
    Json,
    Yaml,
}

impl Format {
    fn from_path(path: &Path) -> Result<Self> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("json") => Ok(Self::Json),
            Some("yaml") | Some("yml") => Ok(Self::Yaml),
            Some(other) => bail!(
                "unsupported --results-file extension: .{other} (expected .json, .yaml, or .yml)"
            ),
            None => bail!("--results-file must have a .json, .yaml, or .yml extension"),
        }
    }
}

pub fn write_reports(path: &Path, reports: &[SimulationReport]) -> Result<()> {
    let bytes = match Format::from_path(path)? {
        Format::Json => serde_json::to_vec_pretty(reports)?,
        Format::Yaml => serde_yaml::to_string(reports)?.into_bytes(),
    };
    std::fs::write(path, bytes)
        .map_err(|source| eyre::eyre!("writing {}: {source}", path.display()))?;
    Ok(())
}
