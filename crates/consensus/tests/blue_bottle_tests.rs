// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! BlueBottle integration tests.
//!
//! BlueBottle (see `Protocol::blue_bottle`) is pipelined (leader per round) with
//! `wave_length = 2`, `strong_quorum = 4n/5 + 1`, and `anchor_link_size = 2n/5 + 1`.
//! All tests use `n = 10` so the integer quorums come out to:
//!   - strong = direct_commit_quorum = direct_skip_quorum = 9
//!   - anchor link = weak quorum = 5
//!   - "one-fault" surrogate (total - strong + 1) = 2
//!
//! That sizing gives clean headroom for the vote-split scenarios.

use std::num::NonZeroUsize;

use consensus::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{Block, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee},
};

const WAVE_LENGTH: u64 = 2;
const COMMITTEE_SIZE: usize = 10;

fn build_protocol(committee: &Committee) -> Protocol {
    Protocol::blue_bottle(committee.total_stake(), NonZeroUsize::new(1).unwrap())
}

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
    // Decision round of the first leader is round 1 + (wave_length - 1) = WAVE_LENGTH.
    build_dag(&committee, &mut storage, None, WAVE_LENGTH);

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
        sequence[0]
    {
        assert_eq!(block.author(), leader_elector.elect_leader(1));
    } else {
        panic!("Expected a committed leader")
    };
}

/// Ensure idempotent replies.
#[test]
#[tracing_test::traced_test]
fn idempotence() {
    let committee = committee(COMMITTEE_SIZE);
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
    build_dag(&committee, &mut storage, None, 5);

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let committed = committer.try_commit(last_committed).collect::<Vec<_>>();

    let last = committed.into_iter().last().unwrap();
    let last_committed = Some((last.round(), last.authority()));
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

/// Commit one by one each leader as the dag progresses in ideal conditions.
#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());

    let mut last_committed: Option<(RoundNumber, Authority)> = None;
    for n in 1..=10 {
        let enough_blocks = n + (WAVE_LENGTH - 1);
        let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, enough_blocks);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            build_protocol(&committee),
        );

        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        let leader_round = n;
        if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
            sequence[0]
        {
            assert_eq!(block.author(), leader_elector.elect_leader(leader_round));
        } else {
            panic!("Expected a committed leader")
        }

        let last = sequence.into_iter().last().unwrap();
        last_committed = Some((last.round(), last.authority()));
    }
}

/// Commit 10 leaders in a row (calling the committer after adding them).
#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());

    let n = 10;
    let enough_blocks = n + (WAVE_LENGTH - 1);
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
    build_dag(&committee, &mut storage, None, enough_blocks);

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), n as usize);
    for (index, leader_block) in sequence.iter().enumerate() {
        let leader_round = 1 + index as u64;
        if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) =
            leader_block
        {
            assert_eq!(block.author(), leader_elector.elect_leader(leader_round));
        } else {
            panic!("Expected a committed leader")
        };
    }
}

/// Do not commit anything before the first decision round.
#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let first_commit_round = WAVE_LENGTH - 1;
    for r in 0..first_commit_round {
        let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, r);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            build_protocol(&committee),
        );

        let last_committed: Option<(RoundNumber, Authority)> = None;
        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}

/// Skip the first leader when it never proposes a block.
#[test]
#[tracing_test::traced_test]
fn no_leader() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let leader_1 = leader_elector.elect_leader(leader_round_1);

    let genesis: Vec<_> = committee
        .authorities()
        .map(|authority| *Block::genesis(authority).reference())
        .collect();
    let connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1)
        .map(|authority| (authority, genesis.clone()));
    let references_at_round_1 = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_round_1),
        decision_round_1,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round) =
        sequence[0]
    {
        assert_eq!(leader, leader_1);
        assert_eq!(round, leader_round_1);
    } else {
        panic!("Expected to directly skip the leader");
    }
}

/// Directly skip the first leader when no one votes for it.
#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    let references_without_leader_1: Vec<_> = references_1
        .into_iter()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let decision_round_1 = WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_without_leader_1),
        decision_round_1,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round) =
        sequence[0]
    {
        assert_eq!(leader, leader_elector.elect_leader(leader_round_1));
        assert_eq!(round, leader_round_1);
    } else {
        panic!("Expected to directly skip the leader");
    }
}

/// Indirect-commit the first leader via a later anchor.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 4 * total / 5 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Round 1: full DAG (leader L1 exists).
    let leader_round_1 = 1;
    let references_at_round_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    let references_without_leader_1: Vec<_> = references_at_round_1
        .iter()
        .cloned()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // Round 2: split. Only strong_quorum - 1 = 8 authorities link to L1; the remaining 2
    // (a "one-fault" minority) don't. With strong_quorum = 9, L1 misses direct commit
    // but has enough weak supporters for an indirect commit via a later anchor.
    let supporters: Vec<(Authority, Vec<_>)> = committee
        .authorities()
        .take((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_at_round_1.clone()))
        .collect();
    let non_supporters: Vec<(Authority, Vec<_>)> = committee
        .authorities()
        .skip((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let connections_round_2: Vec<_> = supporters.into_iter().chain(non_supporters).collect();
    let references_at_round_2 = build_dag_layer(connections_round_2, &mut storage);

    // Build forward enough to commit a later leader as anchor for L1. With wave_length = 2
    // the next BaseCommitter-B leader (same round_offset as L1) sits at round 3; its
    // decision round is round 4.
    let decision_round_3 = 2 * WAVE_LENGTH + 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_round_2),
        decision_round_3,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    // Sanity: L1 must be committed (directly or indirectly).
    let leader_1 = leader_elector.elect_leader(leader_round_1);
    let first = &sequence[0];
    if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = first {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!("Expected the first leader to be committed, got {first:?}");
    }
}

/// Commit the first leaders, indirect-skip a later one whose supporters are too few.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let one_fault = total - (4 * total / 5 + 1) + 1; // = total - strong_quorum + 1 = 2 for n=10.
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Build to the leader we want to skip.
    let leader_round_to_skip = WAVE_LENGTH + 1; // First leader of the second BaseCommitter wave.
    let references_at_skip_round = build_dag(&committee, &mut storage, None, leader_round_to_skip);

    let references_without_skipped_leader: Vec<_> = references_at_skip_round
        .iter()
        .cloned()
        .filter(|reference| {
            reference.authority != leader_elector.elect_leader(leader_round_to_skip)
        })
        .collect();

    // Only `one_fault` authorities link to the leader we want to skip.
    let mut references_next_round = Vec::new();
    let with_leader: Vec<(Authority, Vec<_>)> = committee
        .authorities()
        .take(one_fault as usize)
        .map(|authority| (authority, references_at_skip_round.clone()))
        .collect();
    references_next_round.extend(build_dag_layer(with_leader, &mut storage));
    let without_leader: Vec<(Authority, Vec<_>)> = committee
        .authorities()
        .skip(one_fault as usize)
        .map(|authority| (authority, references_without_skipped_leader.clone()))
        .collect();
    references_next_round.extend(build_dag_layer(without_leader, &mut storage));

    let decision_round_final = 3 * WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_next_round),
        decision_round_final,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    // The skipped leader must appear as a Skip in the sequence.
    let skipped_leader = leader_elector.elect_leader(leader_round_to_skip);
    let skip_found = sequence.iter().any(|status| {
        matches!(
            status,
            LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round)
                if *leader == skipped_leader && *round == leader_round_to_skip
        )
    });
    assert!(
        skip_found,
        "expected Skip(leader={skipped_leader:?}, round={leader_round_to_skip}), got {sequence:?}",
    );
}

/// If exactly one authority votes for the leader, neither commit nor skip threshold is met.
#[test]
#[tracing_test::traced_test]
fn undecided() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 4 * total / 5 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    let references_1_without_leader: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // One authority votes for the leader; the rest don't.
    let mut authorities = committee.authorities();
    let leader_connection = vec![(authorities.next().unwrap(), references_1)];
    let non_leader_connections: Vec<_> = authorities
        .take((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_1_without_leader.clone()))
        .collect();
    let connections = leader_connection.into_iter().chain(non_leader_connections);
    let references_at_round_2 = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_round_2),
        decision_round_1,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

/// A Skip yielded once must not be re-yielded when the committer is re-seeded with it.
#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Same DAG as `direct_skip`.
    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_1
        .into_iter()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();
    let decision_round_1 = WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_without_leader_1),
        decision_round_1,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let first = committer.try_commit(None).collect::<Vec<_>>();
    assert!(
        matches!(
            first.last(),
            Some(LeaderStatus::DirectSkip(..) | LeaderStatus::IndirectSkip(..)),
        ),
        "precondition: last decision must be a Skip, got {first:?}",
    );

    let seed = first
        .last()
        .map(|status| (status.round(), status.authority()));
    let second = committer.try_commit(seed).collect::<Vec<_>>();
    assert!(second.is_empty(), "trailing skip re-yielded: {second:?}");
}
