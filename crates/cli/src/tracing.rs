// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs::File, path::PathBuf};

use eyre::{Result, WrapErr};
pub use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt};

/// Our crate names for targeted filtering.
const TARGET_CRATES: &[&str] = &["dag", "consensus", "replica"];

pub struct ReplicaTracing {
    level: LevelFilter,
    log_file: Option<PathBuf>,
}

impl Default for ReplicaTracing {
    fn default() -> Self {
        Self {
            level: LevelFilter::INFO,
            log_file: None,
        }
    }
}

impl ReplicaTracing {
    pub fn new(level: LevelFilter) -> Self {
        Self {
            level,
            log_file: None,
        }
    }

    pub fn with_log_file(mut self, path: Option<PathBuf>) -> Self {
        self.log_file = path;
        self
    }

    /// Only our crates are shown at the requested level; third-party libraries are silenced to
    /// WARN. `RUST_LOG` overrides everything. When a log file is configured, logs go to the file
    /// via a non-blocking background writer instead of stderr — bind the returned guard to a
    /// `_guard` local so its `Drop` flushes buffered lines before the process exits.
    pub fn setup(self) -> Result<Option<WorkerGuard>> {
        let mut filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .from_env_lossy();
        for target in TARGET_CRATES {
            if let Ok(directive) = format!("{target}={}", self.level).parse() {
                filter = filter.add_directive(directive);
            }
        }
        match self.log_file {
            Some(path) => {
                let file = File::create(&path)
                    .wrap_err_with(|| format!("opening log file {}", path.display()))?;
                let (writer, guard) = tracing_appender::non_blocking(file);
                fmt()
                    .with_env_filter(filter)
                    .with_writer(writer)
                    .with_ansi(false)
                    .init();
                Ok(Some(guard))
            }
            None => {
                fmt().with_env_filter(filter).init();
                Ok(None)
            }
        }
    }
}
