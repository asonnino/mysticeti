// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Extra per-message delay drawn from an exponential distribution, capped.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct ExponentialJitter {
    pub mean_ms: u64,
    pub cap_ms: u64,
}

impl ExponentialJitter {
    pub fn sample(&self, rng: &mut impl Rng) -> Duration {
        // Inverse-CDF sample; `1 - uniform` lies in (0, 1], so the logarithm is finite.
        let uniform: f64 = rng.gen_range(0.0..1.0);
        let jitter_ms = -(self.mean_ms as f64) * (1.0 - uniform).ln();
        Duration::from_secs_f64(jitter_ms.min(self.cap_ms as f64) / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{SeedableRng, rngs::StdRng};

    use super::ExponentialJitter;

    #[test]
    fn samples_never_exceed_the_cap() {
        let jitter = ExponentialJitter {
            mean_ms: 75,
            cap_ms: 100,
        };
        let mut rng = StdRng::seed_from_u64(0);
        for _ in 0..10_000 {
            assert!(jitter.sample(&mut rng) <= Duration::from_millis(100));
        }
    }

    #[test]
    fn uncapped_mean_matches() {
        let jitter = ExponentialJitter {
            mean_ms: 75,
            cap_ms: u64::MAX,
        };
        let mut rng = StdRng::seed_from_u64(0);
        let samples = 100_000;
        let total: Duration = (0..samples).map(|_| jitter.sample(&mut rng)).sum();
        let mean_ms = total.as_secs_f64() * 1000.0 / samples as f64;
        assert!((mean_ms - 75.0).abs() < 1.5, "mean {mean_ms} ms");
    }

    #[test]
    fn zero_mean_adds_nothing() {
        let jitter = ExponentialJitter {
            mean_ms: 0,
            cap_ms: 400,
        };
        let mut rng = StdRng::seed_from_u64(0);
        assert_eq!(jitter.sample(&mut rng), Duration::ZERO);
    }
}
