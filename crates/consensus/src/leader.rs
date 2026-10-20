// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use blake2::{Blake2b, Digest};
use dag::{authority::Authority, block::RoundNumber};
use digest::consts::U8;

/// Seed of the [`LeaderElection::FakeCoin`] used by every asynchronous protocol.
pub const DEFAULT_FAKE_COIN_SEED: u64 = 0x6d79_7374_6963_6574;

/// How the leaders of each round are chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderElection {
    /// Round-robin over the committee.
    RoundRobin,
    /// Pseudo-random leaders drawn from a PRF of a seed the validators share and the network does
    /// not know. Sound for crash faults only, as long as the seed stays secret.
    // TODO: replace with a proper random coin (a DKG plus threshold signatures, revealed after
    // the vote round) to tolerate Byzantine validators.
    FakeCoin { seed: u64 },
}

impl LeaderElection {
    /// The fake coin with [`DEFAULT_FAKE_COIN_SEED`].
    pub const fn fake_coin() -> Self {
        Self::FakeCoin {
            seed: DEFAULT_FAKE_COIN_SEED,
        }
    }
}

/// Determines the leaders of each round.
pub struct LeaderElector {
    committee_len: usize,
    election: LeaderElection,
}

impl LeaderElector {
    pub fn new(committee_len: usize, election: LeaderElection) -> Self {
        Self {
            committee_len,
            election,
        }
    }

    /// The leader of slot `offset` of `round`; the slots of one round have distinct leaders as
    /// long as `offset < committee_len`.
    pub fn elect_leader(&self, round: RoundNumber, offset: RoundNumber) -> Authority {
        let committee_len = self.committee_len as u64;
        match self.election {
            LeaderElection::RoundRobin => Authority::new((round + offset) % committee_len),
            LeaderElection::FakeCoin { seed } => {
                // Round-robin from a pseudo-random start, so the slots of a round stay distinct.
                let start = Self::prf(seed, round) % committee_len;
                Authority::new((start + offset % committee_len) % committee_len)
            }
        }
    }

    /// Blake2b keyed by prefixing the seed (Blake2 has no length-extension weakness).
    fn prf(seed: u64, round: RoundNumber) -> u64 {
        let mut hasher = Blake2b::<U8>::new();
        hasher.update(seed.to_be_bytes());
        hasher.update(round.to_be_bytes());
        u64::from_be_bytes(hasher.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::leader::{DEFAULT_FAKE_COIN_SEED, LeaderElection, LeaderElector};

    const FAKE_COIN: LeaderElection = LeaderElection::FakeCoin {
        seed: DEFAULT_FAKE_COIN_SEED,
    };

    /// Round-robin keeps its schedule: slot `offset` of round `r` is `(r + offset) mod n`.
    #[test]
    fn round_robin_schedule() {
        let elector = LeaderElector::new(5, LeaderElection::RoundRobin);
        assert_eq!(elector.elect_leader(0, 0).as_u64(), 0);
        assert_eq!(elector.elect_leader(3, 1).as_u64(), 4);
        assert_eq!(elector.elect_leader(7, 4).as_u64(), 1);
    }

    /// The fake coin is a function of the seed and the round.
    #[test]
    fn fake_coin_is_deterministic() {
        let first = LeaderElector::new(20, FAKE_COIN);
        let second = LeaderElector::new(20, FAKE_COIN);
        for round in 0..100 {
            assert_eq!(first.elect_leader(round, 0), second.elect_leader(round, 0));
        }
    }

    /// The slots of one round have distinct leaders, up to the whole committee.
    #[test]
    fn fake_coin_slots_are_distinct() {
        for committee_len in [1, 4, 20] {
            let elector = LeaderElector::new(committee_len, FAKE_COIN);
            for round in 0..50 {
                let leaders: HashSet<_> = (0..committee_len as u64)
                    .map(|offset| elector.elect_leader(round, offset))
                    .collect();
                assert_eq!(leaders.len(), committee_len);
            }
        }
    }

    /// Over many rounds every authority leads, roughly equally often.
    #[test]
    fn fake_coin_covers_the_committee() {
        let committee_len = 7;
        let elector = LeaderElector::new(committee_len, FAKE_COIN);
        let mut counts = vec![0; committee_len];
        for round in 0..7_000 {
            counts[elector.elect_leader(round, 0).index()] += 1;
        }
        for count in counts {
            assert!((700..1300).contains(&count), "unbalanced election: {count}");
        }
    }

    /// Different seeds give different schedules.
    #[test]
    fn fake_coin_depends_on_the_seed() {
        let first = LeaderElector::new(20, FAKE_COIN);
        let second = LeaderElector::new(20, LeaderElection::FakeCoin { seed: 1 });
        let differs =
            (0..100).any(|round| first.elect_leader(round, 0) != second.elect_leader(round, 0));
        assert!(differs);
    }
}
