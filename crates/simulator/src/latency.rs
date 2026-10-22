// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, ops::Range, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};

mod geography;
mod jitter;
mod uniform;

pub use geography::Geography;
pub use jitter::ExponentialJitter;
pub use uniform::UniformLatency;

/// How long messages take on each directed link.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum LatencyModel {
    Uniform(UniformLatency),
    Geographic(Geography),
}

impl Default for LatencyModel {
    fn default() -> Self {
        Self::Uniform(UniformLatency::default())
    }
}

impl LatencyModel {
    pub fn validate(&self) -> Result<(), LatencyError> {
        match self {
            Self::Uniform(uniform) => uniform.validate(),
            Self::Geographic(geography) => geography.validate(),
        }
    }

    /// Expects a model that passes [`Self::validate`], which `SimulatedNetwork::new` enforces.
    pub fn link(&self, from: usize, to: usize) -> LinkLatency {
        match self {
            Self::Uniform(uniform) => uniform.link(),
            Self::Geographic(geography) => geography
                .link(from, to)
                .expect("latency model must be validated"),
        }
    }
}

impl fmt::Display for LatencyModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uniform(uniform) => uniform.fmt(f),
            Self::Geographic(geography) => geography.fmt(f),
        }
    }
}

/// Latency of one directed link: a fixed base plus a uniformly drawn extra, plus optional jitter.
#[derive(Clone, Debug)]
pub struct LinkLatency {
    base: Duration,
    extra: Range<Duration>,
    jitter: Option<ExponentialJitter>,
}

impl LinkLatency {
    pub fn new(base: Duration, extra: Range<Duration>) -> Self {
        Self {
            base,
            extra,
            jitter: None,
        }
    }

    pub fn with_jitter(mut self, jitter: Option<ExponentialJitter>) -> Self {
        self.jitter = jitter;
        self
    }

    /// An empty `extra` range and no jitter draw nothing from `rng`.
    pub fn sample(&self, rng: &mut impl Rng) -> Duration {
        let extra = if self.extra.is_empty() {
            self.extra.start
        } else {
            rng.gen_range(self.extra.clone())
        };
        let jitter = self
            .jitter
            .map_or(Duration::ZERO, |jitter| jitter.sample(rng));
        self.base + extra + jitter
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LatencyError {
    #[error("latency range is inverted: {start:?} exceeds {end:?}")]
    InvertedRange { start: Duration, end: Duration },
    #[error("geography lists no regions")]
    EmptyRegions,
    #[error("no RTT between regions {from} and {to}")]
    MissingRtt { from: String, to: String },
    #[error("invalid latency of {0} ms")]
    InvalidLatency(f64),
}
