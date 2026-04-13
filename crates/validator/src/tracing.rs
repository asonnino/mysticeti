// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt};

/// Our crate names for targeted filtering.
const OUR_CRATES: &[&str] = &["dag", "consensus", "validator"];

pub struct ValidatorTracing;

impl ValidatorTracing {
    /// Set up tracing with the default level (INFO).
    /// `RUST_LOG` env var takes precedence.
    pub fn setup() {
        Self::setup_with_level(LevelFilter::INFO);
    }

    /// Set up tracing with an explicit level.
    /// Only our crates are shown at the requested level; third-party
    /// libraries are silenced to WARN. `RUST_LOG` overrides everything.
    pub fn setup_with_level(level: LevelFilter) {
        let mut filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .from_env_lossy();
        for our_crate in OUR_CRATES {
            if let Ok(directive) = format!("{our_crate}={level}").parse() {
                filter = filter.add_directive(directive);
            }
        }
        fmt().with_env_filter(filter).init();
    }
}
