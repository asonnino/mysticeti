// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run-results exporter.
//!
//! [`Exporter::new`] takes a user-given `output_dir`, creates it if needed, and owns
//! the file-layout convention for everything written there:
//!
//! - `<output_dir>/`[`TRACING_LOG_FILE`] — suite-wide tracing log; the path is
//!   surfaced via [`Exporter::tracing_log_path`] so the tracing subscriber can be
//!   pointed at it.
//! - `<output_dir>/<run>/`[`CONFIG_FILE`] — run config (serde_yaml).
//! - `<output_dir>/<run>/`[`META_FILE`] — `{ outcome, duration_secs, kind, timestamp_unix }`.
//! - `<output_dir>/<run>/metrics-{authority}.prom` — one file per replica
//!   (e.g. `metrics-A.prom`, `metrics-B.prom`). Each file is a self-contained
//!   Prometheus text exposition; consumers can `promtool check metrics` per
//!   file or merge them into their analysis layer.
//! - `<output_dir>/<run>/`[`DAG_FILE`] — opt-in committed sub-DAG dump (one JSON
//!   line per commit). Written **during** the run by `SimulationRunner::with_dag_writer`,
//!   not by the exporter; the exporter only resolves the per-run path.
//!
//! `<run>` is the per-run subdirectory: in single-run invocations it collapses to
//! `output_dir` itself; in multi-run suites it's the run's `name` (sanitised) or its
//! 1-based index.
//!
//! Per-file writes go through `NamedTempFile` + `persist`, so a mid-write crash leaves
//! the destination either intact (previous content / absent) or fully updated — never
//! half-written.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use dag::{
    authority::Authority,
    metrics::{Outcome, RunKind, RunResult},
};
use eyre::{Result, WrapErr};
use serde::Serialize;
use tempfile::NamedTempFile;

/// File-layout convention for `<output_dir>/<run>/`. Kept private — every artefact
/// is exposed through a dedicated accessor on [`Exporter`].
const CONFIG_FILE: &str = "config.yaml";
const META_FILE: &str = "meta.yaml";
const DAG_FILE: &str = "dag.ndjson";
const TRACING_LOG_FILE: &str = "tracing.log";

#[derive(Serialize)]
struct Meta {
    outcome: Outcome,
    duration_secs: u64,
    kind: RunKind,
    /// Unix epoch seconds at the time of export. Downstream consumers render as RFC3339
    /// on their end (e.g. Python `datetime.fromtimestamp(ts, tz=timezone.utc)`).
    timestamp_unix: u64,
}

pub struct Exporter {
    output_dir: PathBuf,
}

impl Exporter {
    /// Construct the exporter and ensure `output_dir` exists. Surfaces filesystem
    /// errors (e.g. permission denied) before the simulation starts.
    pub fn new(output_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&output_dir)
            .wrap_err_with(|| format!("creating {}", output_dir.display()))?;
        Ok(Self { output_dir })
    }

    /// Path to the suite-wide tracing log inside the output dir.
    pub fn tracing_log_path(&self) -> PathBuf {
        self.output_dir.join(TRACING_LOG_FILE)
    }

    /// Path to the per-run committed-sub-DAG dump. Creates the per-run dir if needed.
    pub fn dag_path(&self, index: usize, total: usize, name: Option<&str>) -> Result<PathBuf> {
        Ok(self.create_run_dir(index, total, name)?.join(DAG_FILE))
    }

    /// Per-run subdirectory under `output_dir`. Single-run invocations (`total == 1`)
    /// return `output_dir` verbatim; multi-run suites return `<output_dir>/<slug>`,
    /// where the slug is `name` (sanitised) when set, else the 1-based `index`. The
    /// directory is created if it doesn't already exist.
    pub fn create_run_dir(
        &self,
        index: usize,
        total: usize,
        name: Option<&str>,
    ) -> Result<PathBuf> {
        let dir = if total == 1 {
            self.output_dir.clone()
        } else {
            let slug = name
                .map(|n| {
                    n.chars()
                        .map(|c| {
                            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                                c
                            } else {
                                '-'
                            }
                        })
                        .collect::<String>()
                })
                .unwrap_or_else(|| index.to_string());
            self.output_dir.join(slug)
        };
        std::fs::create_dir_all(&dir).wrap_err_with(|| format!("creating {}", dir.display()))?;
        Ok(dir)
    }

    /// Per-replica metrics filename inside the per-run dir. Single source of
    /// truth for the `metrics-{authority}.prom` layout — change here when the
    /// convention moves.
    fn metrics_filename(authority: Authority) -> String {
        format!("metrics-{authority}.prom")
    }

    /// Write `config.yaml`, `meta.yaml`, and one `metrics-{authority}.prom`
    /// per replica into the per-run dir resolved via
    /// [`create_run_dir`](Self::create_run_dir).
    pub fn write_to<C: Serialize>(
        &self,
        result: &RunResult<C>,
        index: usize,
        total: usize,
        name: Option<&str>,
    ) -> Result<()> {
        let dir = self.create_run_dir(index, total, name)?;

        Self::write_atomic(&dir, CONFIG_FILE, |writer| {
            serde_yaml::to_writer(writer, &result.config)
                .map_err(|e| std::io::Error::other(e.to_string()))
        })?;

        let meta = Meta {
            outcome: result.outcome,
            duration_secs: result.duration.as_secs(),
            kind: result.kind,
            timestamp_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        Self::write_atomic(&dir, META_FILE, |writer| {
            serde_yaml::to_writer(writer, &meta).map_err(|e| std::io::Error::other(e.to_string()))
        })?;

        // One file per replica: each is self-contained valid Prometheus text. Combining
        // into a single file would duplicate `# HELP` / `# TYPE` blocks and yield
        // exposition that `promtool` rejects.
        for (i, snapshot) in result.metrics.iter().enumerate() {
            let filename = Self::metrics_filename(Authority::from(i));
            Self::write_atomic(&dir, &filename, |writer| {
                writer.write_all(snapshot.to_prometheus_text().as_bytes())
            })?;
        }
        Ok(())
    }

    /// Stage `write` into a `NamedTempFile` in `dir`, then atomically rename to
    /// `dir/file_name`. A mid-write crash leaves the destination intact-or-absent —
    /// never half-written.
    fn write_atomic(
        dir: &Path,
        file_name: &str,
        write: impl FnOnce(&mut BufWriter<&File>) -> std::io::Result<()>,
    ) -> Result<()> {
        let temp = NamedTempFile::new_in(dir)
            .wrap_err_with(|| format!("creating temp file in {}", dir.display()))?;
        {
            let mut writer = BufWriter::new(temp.as_file());
            write(&mut writer).wrap_err_with(|| format!("writing {file_name}"))?;
            writer
                .flush()
                .wrap_err_with(|| format!("flushing {file_name}"))?;
        }
        let path = dir.join(file_name);
        temp.persist(&path)
            .map_err(|e| eyre::eyre!("persisting {}: {}", path.display(), e.error))?;
        Ok(())
    }
}
