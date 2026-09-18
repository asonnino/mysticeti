// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt, ops::Range, time::Duration};

use serde::{Deserialize, Serialize};

use super::{LatencyError, LinkLatency};

/// Longest latency a geography may list.
const MAX_LATENCY: Duration = Duration::from_secs(3600);

/// A link takes half the RTT between its endpoints' regions, plus a small uniform extra.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Geography {
    /// Authority `i` sits in `regions[i % regions.len()]`, as on the testbed.
    pub regions: Vec<String>,
    /// `rtt_ms[from][to]`; a missing direction falls back to the reverse one.
    pub rtt_ms: BTreeMap<String, BTreeMap<String, f64>>,
    /// Uniform per-message extra on top of the half RTT (processing time and jitter).
    #[serde(default = "defaults::extra_ms")]
    pub extra_ms: Range<f64>,
}

impl Geography {
    pub fn region_of(&self, authority: usize) -> Option<&str> {
        let slot = authority.checked_rem(self.regions.len())?;
        Some(&self.regions[slot])
    }

    /// An absent intra-region RTT is zero.
    pub fn round_trip_ms(&self, from: &str, to: &str) -> Option<f64> {
        let lookup = |a: &str, b: &str| self.rtt_ms.get(a)?.get(b).copied();
        lookup(from, to)
            .or_else(|| lookup(to, from))
            .or((from == to).then_some(0.0))
    }

    pub fn link(&self, from: usize, to: usize) -> Result<LinkLatency, LatencyError> {
        let from = self.region_of(from).ok_or(LatencyError::EmptyRegions)?;
        let to = self.region_of(to).ok_or(LatencyError::EmptyRegions)?;
        let round_trip = self
            .round_trip_ms(from, to)
            .ok_or_else(|| LatencyError::MissingRtt {
                from: from.to_string(),
                to: to.to_string(),
            })?;
        let one_way = duration_from_ms(round_trip)? / 2;
        Ok(LinkLatency::new(one_way, self.extra()?))
    }

    /// Checks that the link between every pair of regions resolves.
    pub fn validate(&self) -> Result<(), LatencyError> {
        if self.regions.is_empty() {
            return Err(LatencyError::EmptyRegions);
        }
        // Authority `i` sits in slot `i % len`, so the first `len` authorities cover every link.
        for from in 0..self.regions.len() {
            for to in 0..self.regions.len() {
                self.link(from, to)?;
            }
        }
        Ok(())
    }

    fn extra(&self) -> Result<Range<Duration>, LatencyError> {
        let start = duration_from_ms(self.extra_ms.start)?;
        let end = duration_from_ms(self.extra_ms.end)?;
        if start > end {
            return Err(LatencyError::InvertedRange { start, end });
        }
        Ok(start..end)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Geography {
    /// Regions `a` to `d` are 20 ms apart and `far` is 200 ms from each of them.
    pub fn new_for_test() -> Self {
        let regions = ["a", "b", "c", "d", "far"].map(String::from).to_vec();
        let mut rtt_ms = BTreeMap::new();
        for (index, from) in regions.iter().enumerate() {
            let row: BTreeMap<_, _> = regions[index + 1..]
                .iter()
                .map(|to| (to.clone(), if to == "far" { 200.0 } else { 20.0 }))
                .collect();
            rtt_ms.insert(from.clone(), row);
        }
        Self {
            regions,
            rtt_ms,
            extra_ms: defaults::extra_ms(),
        }
    }
}

impl fmt::Display for Geography {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let regions = &self.regions;
        let round_trips = || {
            let pairs = regions
                .iter()
                .flat_map(|from| regions.iter().map(move |to| (from, to)));
            pairs.filter_map(|(from, to)| self.round_trip_ms(from, to))
        };
        let min = round_trips().min_by(f64::total_cmp).unwrap_or_default();
        let max = round_trips().max_by(f64::total_cmp).unwrap_or_default();
        write!(
            f,
            "{} regions, RTT {min}-{max} ms, extra {}-{} ms",
            regions.len(),
            self.extra_ms.start,
            self.extra_ms.end
        )
    }
}

fn duration_from_ms(milliseconds: f64) -> Result<Duration, LatencyError> {
    Duration::try_from_secs_f64(milliseconds / 1000.0)
        .ok()
        .filter(|latency| *latency <= MAX_LATENCY)
        .ok_or(LatencyError::InvalidLatency(milliseconds))
}

mod defaults {
    use std::ops::Range;

    pub fn extra_ms() -> Range<f64> {
        0.0..1.0
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use rand::{SeedableRng, rngs::StdRng};

    use super::Geography;
    use crate::latency::LatencyError;

    #[test]
    fn placement_is_round_robin() {
        let geography = Geography {
            regions: [
                "us-east-1",
                "us-east-2",
                "eu-central-1",
                "eu-west-2",
                "eu-west-3",
                "tokyo",
            ]
            .map(String::from)
            .to_vec(),
            ..Geography::new_for_test()
        };
        let tokyo = |committee_size: usize| -> Vec<usize> {
            (0..committee_size)
                .filter(|authority| geography.region_of(*authority) == Some("tokyo"))
                .collect()
        };
        assert_eq!(tokyo(50), vec![5, 11, 17, 23, 29, 35, 41, 47]);
        assert_eq!(tokyo(10), vec![5]);
    }

    #[test]
    fn rtt_lookup_falls_back() {
        let geography = Geography::new_for_test();
        assert_eq!(geography.round_trip_ms("a", "far"), Some(200.0));
        assert_eq!(geography.round_trip_ms("far", "a"), Some(200.0));
        assert_eq!(geography.round_trip_ms("far", "far"), Some(0.0));
        assert_eq!(geography.round_trip_ms("a", "unknown"), None);
    }

    #[test]
    fn links_take_half_the_rtt_plus_extra() {
        let geography = Geography::new_for_test();
        let mut rng = StdRng::seed_from_u64(0);
        let extra = Duration::from_millis(1);
        // Authorities 4 and 9 are both in `far`; authority 0 is in `a`.
        let cases = [(4, 9, Duration::ZERO), (9, 0, Duration::from_millis(100))];
        for (from, to, base) in cases {
            let link = geography.link(from, to).unwrap();
            for _ in 0..1_000 {
                let latency = link.sample(&mut rng);
                assert!(base <= latency && latency < base + extra);
            }
        }
    }

    #[test]
    fn zero_extra_is_exactly_half_the_rtt() {
        let geography = Geography {
            extra_ms: 0.0..0.0,
            ..Geography::new_for_test()
        };
        let link = geography.link(0, 1).unwrap();
        let mut rng = StdRng::seed_from_u64(0);
        assert_eq!(link.sample(&mut rng), Duration::from_millis(10));
    }

    #[test]
    fn display_covers_listed_regions_only() {
        let mut geography = Geography::new_for_test();
        let stale_row = BTreeMap::from([("a".to_string(), 999.0)]);
        geography.rtt_ms.insert("stale".to_string(), stale_row);
        assert_eq!(
            geography.to_string(),
            "5 regions, RTT 0-200 ms, extra 0-1 ms"
        );
    }

    #[test]
    fn invalid_geographies_are_rejected() {
        let no_regions = Geography {
            regions: Vec::new(),
            ..Geography::new_for_test()
        };
        let mut unknown_region = Geography::new_for_test();
        unknown_region.regions.push("unknown".to_string());
        let with_far_rtt = |round_trip_ms: f64| {
            let mut geography = Geography::new_for_test();
            let row = geography.rtt_ms.get_mut("a").unwrap();
            row.insert("far".to_string(), round_trip_ms);
            geography
        };
        let negative_rtt = with_far_rtt(-1.0);
        let absurd_rtt = with_far_rtt(1e22);
        let inverted_extra = Geography {
            extra_ms: 2.0..1.0,
            ..Geography::new_for_test()
        };

        let error = |geography: &Geography| geography.validate().unwrap_err();
        assert!(matches!(error(&no_regions), LatencyError::EmptyRegions));
        assert!(matches!(
            error(&unknown_region),
            LatencyError::MissingRtt { .. }
        ));
        assert!(matches!(
            error(&negative_rtt),
            LatencyError::InvalidLatency(_)
        ));
        assert!(matches!(
            error(&absurd_rtt),
            LatencyError::InvalidLatency(_)
        ));
        assert!(matches!(
            error(&inverted_extra),
            LatencyError::InvertedRange { .. }
        ));
        assert!(Geography::new_for_test().validate().is_ok());
    }
}
