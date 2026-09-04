// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The canary leader wait: which rounds wait, and for whom. The waited
//! cohort is always the public round-robin schedule — never the coin leader.

use std::num::{NonZeroU64, NonZeroUsize};

use consensus::{
    committer::Committer,
    protocol::{ConsensusProtocol, SteelheadPair},
};
use dag::{
    authority::Authority, block::RoundNumber, consensus::DagConsensus, storage::Storage,
    test_util::committee,
};

const COMMITTEE_SIZE: u64 = 4;

fn committer(period: Option<u64>, canary: Option<u64>, leader_count: usize) -> Committer {
    let committee = committee(COMMITTEE_SIZE as usize);
    let storage = Storage::new_for_test(&committee);
    let spec = ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: period.and_then(NonZeroU64::new),
        async_wave_length: 5,
        adaptive: None,
        canary: canary.and_then(NonZeroU64::new),
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
    };
    Committer::new_for_test(&committee, &storage, &spec)
}

fn waited_cohort(committer: &Committer, round: RoundNumber) -> Option<Vec<Authority>> {
    committer
        .get_leaders(round)
        .map(|leaders| leaders.collect())
}

fn round_robin(round: RoundNumber) -> Authority {
    Authority::new(round % COMMITTEE_SIZE)
}

#[test]
fn default_canary_waits_on_every_round() {
    let committer = committer(Some(4), Some(1), 1);
    for round in 1..=12 {
        assert_eq!(
            waited_cohort(&committer, round),
            Some(vec![round_robin(round)]),
            "round {round}"
        );
    }
}

#[test]
fn canary_waits_for_round_robin_not_the_coin() {
    let committer = committer(Some(4), Some(1), 1);
    // Round 4 is an async slot: its decided leader is the fake coin's, but
    // the wait targets the public round-robin authority.
    let slot_leader = committer.leader_at(4, 0);
    let cohort = waited_cohort(&committer, 4).expect("canaried round waits");
    assert_eq!(cohort, vec![round_robin(4)]);
    assert_ne!(slot_leader, round_robin(4), "coin leader must stay hidden");
}

#[test]
fn sampled_canary_skips_off_cycle_async_rounds() {
    // Period 1: every round is async; canary 2 waits on even rounds only.
    let committer = committer(Some(1), Some(2), 1);
    for round in 1..=12u64 {
        let expected = round.is_multiple_of(2).then(|| vec![round_robin(round)]);
        assert_eq!(waited_cohort(&committer, round), expected, "round {round}");
    }
}

#[test]
fn disabled_canary_restores_pure_quorum_pacing() {
    let committer = committer(Some(4), None, 1);
    for round in 1..=12u64 {
        let expected = (!round.is_multiple_of(4)).then(|| vec![round_robin(round)]);
        assert_eq!(waited_cohort(&committer, round), expected, "round {round}");
    }
}

#[test]
fn pure_sync_waits_regardless_of_canary() {
    for canary in [Some(1), None] {
        let committer = committer(None, canary, 1);
        for round in 1..=12 {
            assert_eq!(
                waited_cohort(&committer, round),
                Some(vec![round_robin(round)]),
                "round {round}, canary {canary:?}"
            );
        }
    }
}

#[test]
fn cohort_covers_all_leader_offsets() {
    let committer = committer(Some(4), Some(1), 2);
    for round in 1..=8 {
        assert_eq!(
            waited_cohort(&committer, round),
            Some(vec![round_robin(round), round_robin(round + 1)]),
            "round {round}"
        );
    }
}
