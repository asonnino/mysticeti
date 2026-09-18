// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, ops::Range, time::Duration};

use serde::{Deserialize, Serialize};

use super::{LatencyError, LinkLatency};

/// Every link draws uniformly from the same range.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UniformLatency {
    #[serde(default = "defaults::range_ms")]
    pub range_ms: Range<u64>,
}

impl Default for UniformLatency {
    fn default() -> Self {
        Self {
            range_ms: defaults::range_ms(),
        }
    }
}

impl UniformLatency {
    pub fn validate(&self) -> Result<(), LatencyError> {
        let Range { start, end } = self.range();
        if start > end {
            return Err(LatencyError::InvertedRange { start, end });
        }
        Ok(())
    }

    pub fn link(&self) -> LinkLatency {
        LinkLatency::new(Duration::ZERO, self.range())
    }

    fn range(&self) -> Range<Duration> {
        Duration::from_millis(self.range_ms.start)..Duration::from_millis(self.range_ms.end)
    }
}

impl fmt::Display for UniformLatency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{} ms", self.range_ms.start, self.range_ms.end)
    }
}

mod defaults {
    use std::ops::Range;

    pub fn range_ms() -> Range<u64> {
        50..100
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, time::Duration};

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::UniformLatency;
    use crate::latency::LatencyError;

    #[test]
    fn draws_match_a_raw_range() {
        let range = Duration::from_millis(50)..Duration::from_millis(100);
        let link = UniformLatency { range_ms: 50..100 }.link();
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
        let link = UniformLatency { range_ms: 80..80 }.link();
        let mut rng = StdRng::seed_from_u64(0);
        assert_eq!(link.sample(&mut rng), Duration::from_millis(80));
    }

    #[test]
    fn inverted_range_is_rejected() {
        let range_ms = Range {
            start: 200,
            end: 100,
        };
        let error = UniformLatency { range_ms }.validate().unwrap_err();
        assert!(matches!(error, LatencyError::InvertedRange { .. }));
    }
}
