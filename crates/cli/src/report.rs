// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use dag::{authority::Authority, metrics::ReplicaStats};
use eyre::{Result, WrapErr, bail};
use serde::Serialize;
use simulator::{SimulationConfig, SimulationResults};
use tempfile::NamedTempFile;

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
                    stats: metrics.replica_stats(),
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

/// Write `reports` atomically: serialise, stream into a sibling temp file, then rename into
/// place. A Ctrl-C or disk error mid-write leaves the destination either intact (previous
/// content / absent) or fully updated — never half-written.
pub fn write_reports(path: &Path, reports: &[SimulationReport]) -> Result<()> {
    let format = Format::from_path(path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).wrap_err_with(|| format!("creating {}", parent.display()))?;
    let mut temp = NamedTempFile::new_in(parent)
        .wrap_err_with(|| format!("creating temp file in {}", parent.display()))?;
    match format {
        Format::Json => serde_json::to_writer_pretty(&mut temp, reports)?,
        Format::Yaml => serde_yaml::to_writer(&mut temp, reports)?,
    }
    temp.persist(path)
        .map_err(|error| eyre::eyre!("writing {}: {}", path.display(), error.error))?;
    Ok(())
}
