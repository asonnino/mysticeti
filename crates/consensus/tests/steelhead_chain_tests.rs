// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The chain verdicts behind the adaptive period, and the handover between the
//! two rules. Under asynchrony an adversary holds every known-leader slot
//! undecided at no cost: the leader block reaches exactly f + 1 validators
//! before they propose at the vote round, so neither n - f certificates nor
//! n - f blames ever form. The output then stalls below the first such slot,
//! and a trigger read from the output would never fire. The chain verdicts
//! (the asynchronous rule read on every round with its coin leader) keep
//! moving, the period falls to 1, and the asynchronous rule recovers every
//! slot below.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, atomic::AtomicU64, atomic::Ordering},
};

use consensus::{
    committer::Committer,
    protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair},
};
use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee, drop_leader},
};

const INTERVAL: u64 = 32;
const MAX_PERIOD: u64 = 4;
const ASYNC_WAVE_LENGTH: u64 = 5;

fn adaptive_spec() -> ConsensusProtocol {
    ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: None,
        async_wave_length: ASYNC_WAVE_LENGTH,
        adaptive: Some(AdaptiveConfig {
            interval: INTERVAL,
            max_period: NonZeroU64::new(MAX_PERIOD).unwrap(),
            epsilon_percent: 10,
        }),
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(1).unwrap(),
    }
}

/// The partial-dissemination attack shape under period `MAX_PERIOD`: the
/// round-robin leader block of every synchronous round is referenced by
/// exactly f + 1 = 2 next-round blocks (its own author's and one helper's),
/// while the rounds after an asynchronous round are fully connected. Every
/// synchronous slot is therefore directly undecided, forever; the fake-coin
/// leader of any round is a different author whose block everyone references.
fn build_held_undecided_dag(committee: &Arc<Committee>, storage: &mut Storage, depth: u64) {
    let n = committee.len() as u64;
    let mut references = build_dag(committee, storage, None, 0);
    for round in 1..=depth {
        let previous = round - 1;
        let starved = previous >= 1 && previous % MAX_PERIOD != 0;
        let leader = Authority::new(previous % n);
        let helper = Authority::new((previous + 1) % n);
        let connections = committee
            .authorities()
            .map(|authority| {
                let parents = if starved && authority != leader && authority != helper {
                    drop_leader(&references, leader)
                } else {
                    references.clone()
                };
                (authority, parents)
            })
            .collect();
        references = build_dag_layer(connections, storage);
    }
}

/// Drain a committer to exhaustion, consuming at most `chunk` decisions per
/// `try_commit` call, and return the full verdict sequence.
fn consume(committer: &mut Committer, chunk: usize, bound: usize) -> Vec<LeaderStatus> {
    let mut sequence: Vec<LeaderStatus> = Vec::new();
    let mut last_decided: Option<(RoundNumber, Authority)> = None;
    loop {
        let step: Vec<_> = committer.try_commit(last_decided).take(chunk).collect();
        let Some(last) = step.last() else {
            return sequence;
        };
        last_decided = Some((last.round(), last.authority()));
        sequence.extend(step);
        assert!(sequence.len() <= bound, "consumption must terminate");
    }
}

fn reference_of(status: &LeaderStatus) -> Option<BlockReference> {
    match status {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            Some(*block.reference())
        }
        _ => None,
    }
}

/// On the DAG truncated before the switch, the output stalls at round 2: the
/// slot at round 2 waits on the undecided synchronous slot at 6, which waits
/// on 10, and so on, while the asynchronous slots at 4, 8, ... commit directly
/// and are never output. The chain still finds every interval's anchor, so
/// the period for the interval above the DAG's top is already 1.
#[test]
fn held_undecided_sync_slots_stall_the_output() {
    let committee = committee(4);
    let mut storage = Storage::new_for_test(&committee);
    build_held_undecided_dag(&committee, &mut storage, 2 * INTERVAL);

    let cell = Arc::new(AtomicU64::new(MAX_PERIOD));
    let mut committer = Committer::new_for_test(&committee, &storage, &adaptive_spec())
        .with_period_cell(cell.clone());
    let sequence = consume(&mut committer, usize::MAX, 4 * INTERVAL as usize);

    assert_eq!(sequence.len(), 1, "{sequence:?}");
    assert_eq!(sequence[0].round(), 1);
    assert!(matches!(sequence[0], LeaderStatus::IndirectSkip(..)));
    assert_eq!(cell.load(Ordering::Relaxed), 1);
}

/// On the full DAG the period falls to 1 at the boundary of interval 2, every
/// round above it commits directly under the asynchronous rule, and the
/// anchors those commits provide decide every held slot below, so the output
/// resumes and covers every round. Consumption cadence changes nothing.
#[test]
fn held_undecided_sync_slots_do_not_block_the_switch() {
    let committee = committee(4);
    let mut storage = Storage::new_for_test(&committee);
    let depth = 3 * INTERVAL + 4;
    build_held_undecided_dag(&committee, &mut storage, depth);
    let spec = adaptive_spec();

    let mut sequences = Vec::new();
    for chunk in [usize::MAX, 3, 1] {
        let cell = Arc::new(AtomicU64::new(MAX_PERIOD));
        let mut committer =
            Committer::new_for_test(&committee, &storage, &spec).with_period_cell(cell.clone());
        sequences.push(consume(&mut committer, chunk, 2 * depth as usize));
        assert_eq!(cell.load(Ordering::Relaxed), 1, "chunk {chunk} period");
    }

    // The output covers every round whose decision the DAG holds: the last
    // asynchronous wave under period 1 ends at the DAG's top.
    let sequence = &sequences[0];
    let rounds: Vec<RoundNumber> = sequence.iter().map(|status| status.round()).collect();
    let expected: Vec<RoundNumber> = (1..=depth - ASYNC_WAVE_LENGTH + 1).collect();
    assert_eq!(rounds, expected);

    // Every synchronous slot of the first two intervals was held undecided
    // and is decided indirectly, through the anchors the switch provided;
    // every round of the third interval is asynchronous and commits directly.
    for status in sequence {
        let round = status.round();
        if round <= 2 * INTERVAL && round % MAX_PERIOD != 0 {
            assert!(
                matches!(
                    status,
                    LeaderStatus::IndirectSkip(..) | LeaderStatus::IndirectCommit(..)
                ),
                "round {round}: {status:?}"
            );
        } else if round > 2 * INTERVAL {
            assert!(
                matches!(status, LeaderStatus::DirectCommit(..)),
                "round {round}: {status:?}"
            );
        }
    }

    // Agreement across cadences.
    for (index, other) in sequences.iter().enumerate().skip(1) {
        assert_eq!(other.len(), sequence.len(), "cadence {index} length");
        for (position, (status, expected)) in other.iter().zip(sequence.iter()).enumerate() {
            assert_eq!(
                status.round(),
                expected.round(),
                "cadence {index} at {position}"
            );
            assert_eq!(
                status.authority(),
                expected.authority(),
                "cadence {index} at {position}"
            );
            assert_eq!(
                reference_of(status),
                reference_of(expected),
                "cadence {index} at {position}"
            );
        }
    }
}

/// A universe where the synchronous slot at round 1 (leader 1) is directly
/// committed by exactly three certificate blocks at round 3, `c0`, `c1`, `c2`;
/// `c3` references only two votes and carries no certificate. The
/// asynchronous slot at round 4 references `c1`, `c2`, `c3` only, so a view
/// without `c0` is causally complete. Test digests depend on `(round,
/// author)` alone, so building the same layers in two storages yields the same
/// references; `with_c0` decides whether `c0` exists in the view.
fn build_handover_dag(committee: &Arc<Committee>, storage: &mut Storage, with_c0: bool) {
    let leader = Authority::new(1);
    let round1 = build_dag(committee, storage, None, 1);
    // Vote round: authorities 0, 1, 2 vote for the leader; authority 3 does not.
    let votes: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .map(|authority| {
            let parents = if authority == Authority::new(3) {
                drop_leader(&round1, leader)
            } else {
                round1.clone()
            };
            (authority, parents)
        })
        .collect();
    let round2 = build_dag_layer(votes, storage);
    let voters: Vec<BlockReference> = round2[..3].to_vec();
    let non_voters: Vec<BlockReference> = round2[1..].to_vec();
    // Certificate round: c0, c1, c2 carry a certificate; c3 does not.
    let certifiers: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .filter(|authority| with_c0 || *authority != Authority::new(0))
        .map(|authority| {
            let parents = if authority == Authority::new(3) {
                non_voters.clone()
            } else {
                voters.clone()
            };
            (authority, parents)
        })
        .collect();
    let round3 = build_dag_layer(certifiers, storage);
    let without_c0 = drop_leader(&round3, Authority::new(0));
    let round4 = build_dag_layer(
        committee
            .authorities()
            .map(|authority| (authority, without_c0.clone()))
            .collect(),
        storage,
    );
    build_dag(committee, storage, Some(round4), 10);
}

/// Handover: a slot directly committed under the synchronous rule in one view
/// is committed in another view by the indirect step through an asynchronous
/// anchor, with the same block.
#[test]
fn handover_direct_commit_completed_by_asynchronous_anchor() {
    let committee = committee(4);
    let spec = ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: NonZeroU64::new(MAX_PERIOD),
        async_wave_length: ASYNC_WAVE_LENGTH,
        adaptive: None,
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(1).unwrap(),
    };

    let mut full = Storage::new_for_test(&committee);
    build_handover_dag(&committee, &mut full, true);
    let mut partial = Storage::new_for_test(&committee);
    build_handover_dag(&committee, &mut partial, false);

    let mut committer = Committer::new_for_test(&committee, &full, &spec);
    let direct = committer.try_commit(None).next().expect("round 1 decided");
    let mut committer = Committer::new_for_test(&committee, &partial, &spec);
    let indirect = committer.try_commit(None).next().expect("round 1 decided");

    assert_eq!(direct.round(), 1);
    assert_eq!(indirect.round(), 1);
    assert!(
        matches!(direct, LeaderStatus::DirectCommit(..)),
        "{direct:?}"
    );
    assert!(
        matches!(indirect, LeaderStatus::IndirectCommit(..)),
        "{indirect:?}"
    );
    assert_eq!(reference_of(&direct), reference_of(&indirect));
}
