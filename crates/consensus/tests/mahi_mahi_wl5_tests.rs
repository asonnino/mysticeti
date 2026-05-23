// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mahi-Mahi integration tests (`wave_length = 5`).

use std::num::NonZeroUsize;

use consensus::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, build_split_chain, committee},
};

const WAVE_LENGTH: u64 = 5;
const COMMITTEE_SIZE: usize = 4;

fn build_protocol(committee: &Committee) -> Protocol {
    Protocol::mahi_mahi(
        committee.total_stake(),
        NonZeroUsize::new(1).unwrap(),
        WAVE_LENGTH,
    )
    .expect("valid wave_length")
}

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
    // First pipelined leader is at round 1; decision_round = leader_round + wave_length - 1.
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
    build_dag(&committee, &mut storage, None, 2 * WAVE_LENGTH);

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

/// Commit one by one each leader as the DAG progresses in ideal conditions.
#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());

    let mut last_committed: Option<(RoundNumber, Authority)> = None;
    for n in 1..=10u64 {
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

/// Commit 10 leaders in a row.
#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());

    let n = 10u64;
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
    let first_commit_round = WAVE_LENGTH;
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
    let references_pre_leader = build_dag(&committee, &mut storage, None, leader_round_1 - 1);

    let connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1)
        .map(|authority| (authority, references_pre_leader.clone()));
    let references_at_leader_round = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_leader_round),
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
        panic!("Expected to skip the leader");
    }
}

/// Direct-skip the first leader when no voting-round block links to it.
#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
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
        panic!("Expected to skip the leader");
    }
}

/// Commit the first leader via a later anchor when not enough decision-round
/// certificates exist for a direct commit. For `wave_length = 5` the split is
/// chained across two intermediate rounds (R+1 and R+2) before the voting round.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let protocol = build_protocol(&committee);
    let strong_quorum = protocol.direct_commit_quorum as usize;
    let one_fault = (committee.total_stake() / 3 + 1) as usize;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let voting_round = leader_round_1 + WAVE_LENGTH - 2;
    let final_blamers_count = COMMITTEE_SIZE - strong_quorum;
    let (supports_at_voting, blames_at_voting) = build_split_chain(
        &committee,
        &mut storage,
        references_at_leader_round,
        references_without_leader_1,
        voting_round,
        final_blamers_count,
    );

    let mut references_at_decision_round = Vec::new();
    let certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .take(one_fault)
        .map(|authority| (authority, supports_at_voting.clone()))
        .collect();
    references_at_decision_round.extend(build_dag_layer(certifier_connections, &mut storage));

    let mixed_parents: Vec<_> = blames_at_voting
        .into_iter()
        .chain(supports_at_voting)
        .take(strong_quorum)
        .collect();
    let non_certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .skip(one_fault)
        .map(|authority| (authority, mixed_parents.clone()))
        .collect();
    references_at_decision_round.extend(build_dag_layer(non_certifier_connections, &mut storage));

    let next_anchor_decision_round = leader_round_1 + 2 * WAVE_LENGTH - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_decision_round),
        next_anchor_decision_round,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    let leader_1 = leader_elector.elect_leader(leader_round_1);
    let first = &sequence[0];
    if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = first {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!("Expected the first leader to be committed, got {first:?}");
    }
}

/// Indirect-skip the first leader via a later anchor. For `wave_length = 5` the
/// split must be chained through two intermediate rounds; otherwise a full-DAG
/// layer between the leader and the voting round would silently reconnect every
/// block back to the leader via `find_support`'s transitive traversal.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let protocol = build_protocol(&committee);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize; // = 2 for n=4.
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let skipped_leader = leader_elector.elect_leader(leader_round_1);
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != skipped_leader)
        .collect();

    let voting_round = leader_round_1 + WAVE_LENGTH - 2; // = 4 for wl=5.
    let (supports_at_voting, blames_at_voting) = build_split_chain(
        &committee,
        &mut storage,
        references_at_leader_round,
        references_without_leader_1,
        voting_round,
        blamers_count,
    );

    let references_at_voting_round: Vec<_> = supports_at_voting
        .into_iter()
        .chain(blames_at_voting)
        .collect();
    let next_anchor_decision_round = leader_round_1 + 2 * WAVE_LENGTH - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_voting_round),
        next_anchor_decision_round,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    let skip_found = sequence.iter().any(|status| {
        matches!(
            status,
            LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round)
                if *leader == skipped_leader && *round == leader_round_1
        )
    });
    assert!(
        skip_found,
        "expected Skip(leader={skipped_leader:?}, round={leader_round_1}), got {sequence:?}",
    );
}

/// `direct_skip_quorum - 1` blamers and the matching number of supporters at L1's
/// voting round leaves neither commit nor skip thresholds reached, and the DAG
/// doesn't extend far enough to reach a later anchor — L1 stays undecided.
#[test]
#[tracing_test::traced_test]
fn undecided() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let protocol = build_protocol(&committee);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let voting_round = leader_round_1 + WAVE_LENGTH - 2;
    let (supports_at_voting, blames_at_voting) = build_split_chain(
        &committee,
        &mut storage,
        references_at_leader_round,
        references_without_leader_1,
        voting_round,
        blamers_count,
    );
    let references_at_voting_round: Vec<_> = supports_at_voting
        .into_iter()
        .chain(blames_at_voting)
        .collect();

    let decision_round_1 = leader_round_1 + WAVE_LENGTH - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_voting_round),
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

    let leader_round_1 = 1;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
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
