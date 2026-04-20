// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt};

/// Our crate names for targeted filtering.
const TARGET_CRATES: &[&str] = &["dag", "consensus", "replica"];

pub struct ReplicaTracing {
    level: LevelFilter,
}

impl Default for ReplicaTracing {
    fn default() -> Self {
        Self {
            level: LevelFilter::INFO,
        }
    }
}

impl ReplicaTracing {
    pub fn new(level: LevelFilter) -> Self {
        Self { level }
    }

    /// Only our crates are shown at the requested level; third-party
    /// libraries are silenced to WARN. `RUST_LOG` overrides everything.
    pub fn setup(self) {
        let mut filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .from_env_lossy();
        for target in TARGET_CRATES {
            if let Ok(directive) = format!("{target}={}", self.level).parse() {
                filter = filter.add_directive(directive);
            }
        }
        fmt().with_env_filter(filter).init();
    }
}
