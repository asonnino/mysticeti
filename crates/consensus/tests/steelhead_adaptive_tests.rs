// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adaptive Steelhead: the period schedule is a deterministic function of the
//! consumed commit sequence, so committers consuming the same DAG at different
//! cadences must produce identical verdict sequences and period schedules.

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

const DAG_DEPTH: u64 = 80;
const INTERVAL: u64 = 32;
const MAX_PERIOD: u64 = 8;

fn spec() -> ConsensusProtocol {
    ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: None,
        async_wave_length: 5,
        adaptive: Some(AdaptiveConfig {
            interval: INTERVAL,
            max_period: NonZeroU64::new(MAX_PERIOD).unwrap(),
            epsilon_percent: 10,
        }),
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(1).unwrap(),
    }
}

/// The targeted-delay attack shape: every round's blocks exclude the previous
/// round's round-robin leader block, so sync slots skip while async slots
/// (whose fake-coin leader differs from the round-robin one) keep committing.
fn build_leader_starved_dag(committee: &Arc<Committee>, storage: &mut Storage) {
    let n = committee.len() as u64;
    let mut references = build_dag(committee, storage, None, 0);
    for round in 1..=DAG_DEPTH {
        let parents = if round == 1 {
            references.clone()
        } else {
            drop_leader(&references, Authority::new((round - 1) % n))
        };
        references = build_dag_layer(
            committee
                .authorities()
                .map(|authority| (authority, parents.clone()))
                .collect(),
            storage,
        );
    }
}

/// Drain a committer to exhaustion, consuming at most `chunk` decisions per
/// `try_commit` call, and return the full verdict sequence.
fn consume(committer: &mut Committer, chunk: usize) -> Vec<LeaderStatus> {
    let mut sequence: Vec<LeaderStatus> = Vec::new();
    let mut last_decided: Option<(RoundNumber, Authority)> = None;
    loop {
        let step: Vec<_> = committer.try_commit(last_decided).take(chunk).collect();
        let Some(last) = step.last() else {
            return sequence;
        };
        last_decided = Some((last.round(), last.authority()));
        sequence.extend(step);
        assert!(
            sequence.len() <= 2 * DAG_DEPTH as usize,
            "consumption must terminate"
        );
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

#[test]
fn cadence_independent_schedules_and_verdicts() {
    let committee = committee(4);
    let mut storage = Storage::new_for_test(&committee);
    build_leader_starved_dag(&committee, &mut storage);
    let spec = spec();

    let mut sequences = Vec::new();
    let mut cells = Vec::new();
    for chunk in [usize::MAX, 3, 1] {
        let cell = Arc::new(AtomicU64::new(MAX_PERIOD));
        let mut committer =
            Committer::new_for_test(&committee, &storage, &spec).with_period_cell(cell.clone());
        sequences.push(consume(&mut committer, chunk));
        cells.push(cell);
    }

    // Agreement: every cadence yields the same verdicts in the same order.
    for (index, sequence) in sequences.iter().enumerate().skip(1) {
        assert_eq!(sequence.len(), sequences[0].len(), "cadence {index} length");
        for (position, (status, expected)) in sequence.iter().zip(sequences[0].iter()).enumerate() {
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

    // Adaptation: the starved sync slots drive every committer's period to 1,
    // published through its live-period cell.
    for (index, cell) in cells.iter().enumerate() {
        assert_eq!(cell.load(Ordering::Relaxed), 1, "cadence {index} period");
    }

    // The starved shape actually exercises both rules: round-robin sync slots
    // skip while fake-coin async slots commit, deep into the DAG.
    let sequence = &sequences[0];
    assert!(
        sequence
            .iter()
            .any(|status| matches!(status, LeaderStatus::DirectSkip(..)))
    );
    assert!(
        sequence
            .iter()
            .any(|status| matches!(status, LeaderStatus::DirectCommit(..)))
    );
    assert!(
        sequence.last().unwrap().round() >= 30,
        "sequence too short: {sequence:?}"
    );
}
