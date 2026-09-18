// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::Range, time::Duration};

use rand::Rng;

/// Latency of every directed link, resolved once from the config.
#[derive(Clone, Debug)]
pub enum LatencyModel {
    /// Every link draws from the same range.
    Uniform(Range<Duration>),
}

impl LatencyModel {
    pub fn link(&self, _from: usize, _to: usize) -> LinkLatency {
        match self {
            Self::Uniform(range) => LinkLatency {
                base: Duration::ZERO,
                extra: range.clone(),
            },
        }
    }
}

/// Latency of one directed link: a fixed base plus a uniformly drawn extra.
#[derive(Clone, Debug)]
pub struct LinkLatency {
    base: Duration,
    extra: Range<Duration>,
}

impl LinkLatency {
    /// An empty `extra` range draws nothing from `rng`.
    pub fn sample(&self, rng: &mut impl Rng) -> Duration {
        if self.extra.is_empty() {
            return self.base + self.extra.start;
        }
        self.base + rng.gen_range(self.extra.clone())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LatencyError {
    #[error("latency range is inverted: min {min} ms exceeds max {max} ms")]
    InvertedRange { min: f64, max: f64 },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::LatencyModel;

    #[test]
    fn uniform_draws_match_a_raw_range() {
        let range = Duration::from_millis(50)..Duration::from_millis(100);
        let link = LatencyModel::Uniform(range.clone()).link(0, 1);
        let mut model_rng = StdRng::seed_from_u64(7);
        let mut raw_rng = StdRng::seed_from_u64(7);
        for _ in 0..1_000 {
            assert_eq!(
                link.sample(&mut model_rng),
                raw_rng.gen_range(range.clone())
            );
        }
    }

    #[test]
    fn empty_range_is_constant() {
        let latency = Duration::from_millis(80);
        let link = LatencyModel::Uniform(latency..latency).link(0, 1);
        let mut rng = StdRng::seed_from_u64(0);
        assert_eq!(link.sample(&mut rng), latency);
    }
}
