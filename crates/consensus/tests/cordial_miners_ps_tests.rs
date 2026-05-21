// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cordial Miners (Partially Synchronous) integration tests.

use consensus::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee},
};

const WAVE_LENGTH: u64 = 3;
const COMMITTEE_SIZE: usize = 4;

fn build_protocol(committee: &Committee) -> Protocol {
    Protocol::cordial_miners_partially_synchronous(committee.total_stake())
}

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
    // Decision round of the first leader = wave_length + (wave_length - 1) = 2*wave_length - 1.
    build_dag(&committee, &mut storage, None, 2 * WAVE_LENGTH - 1);

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
        assert_eq!(block.author(), leader_elector.elect_leader(WAVE_LENGTH));
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
    build_dag(&committee, &mut storage, None, 2 * WAVE_LENGTH - 1);

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
        let enough_blocks = WAVE_LENGTH * (n + 1) - 1;
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
        let leader_round = n * WAVE_LENGTH;
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
    let enough_blocks = WAVE_LENGTH * (n + 1) - 1;
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
        let leader_round = (index as u64 + 1) * WAVE_LENGTH;
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
    let first_commit_round = 2 * WAVE_LENGTH - 1;
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

/// Skip the first leader when it never proposes a block. With Cordial Miners' unanimous
/// `direct_skip_quorum = n`, all n authorities must blame — which they do because the
/// voting-round blocks all reference the round-2 layer that excludes the missing leader.
#[test]
#[tracing_test::traced_test]
fn no_leader() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Build through the round just before the leader round, then a layer at the leader
    // round that excludes the leader's block, then the decision round.
    let leader_round_1 = WAVE_LENGTH;
    let leader_1 = leader_elector.elect_leader(leader_round_1);
    let references_pre_leader = build_dag(&committee, &mut storage, None, leader_round_1 - 1);

    let connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1)
        .map(|authority| (authority, references_pre_leader.clone()));
    let references_at_leader_round = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = 2 * WAVE_LENGTH - 1;
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

/// Boundary test of the unanimous-blame rule: when every authority's voting-round
/// block omits the leader, `direct_skip_quorum = n` is met and the implementation
/// yields a `DirectSkip`.
#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = WAVE_LENGTH;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .into_iter()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let decision_round_1 = 2 * WAVE_LENGTH - 1;
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

/// Indirect-commit the first leader via a later anchor.
///
/// Construction: full round 3 (with L1) → round 4 with 3 voters + 1 blamer → round 5 split
/// where only some blocks reach the certificate threshold (strong_quorum = 3 votes among
/// their includes); the resulting certificate count at round 5 falls short of strong_quorum,
/// keeping L1 undecided directly. The later wave's leader L2 (round 6) gets direct-committed
/// and serves as the anchor that indirect-commits L1.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let one_fault = total / 3 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = WAVE_LENGTH;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // Round 4 (voting): strong_quorum voters + the rest as blamers.
    let voters: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .take(strong_quorum as usize)
        .map(|authority| (authority, references_at_leader_round.clone()))
        .collect();
    let references_with_votes = build_dag_layer(voters, &mut storage);
    let blamers: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .skip(strong_quorum as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let references_without_votes = build_dag_layer(blamers, &mut storage);

    // Round 5 (certificate split): the first `one_fault` authorities see only voters
    // (and thus produce certificates); the rest see a mixed set with too few voters to
    // certify.
    let mut references_at_certificate_round = Vec::new();
    let certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .take(one_fault as usize)
        .map(|authority| (authority, references_with_votes.clone()))
        .collect();
    references_at_certificate_round.extend(build_dag_layer(certifier_connections, &mut storage));
    let mixed_parents: Vec<_> = references_without_votes
        .into_iter()
        .chain(references_with_votes)
        .take(strong_quorum as usize)
        .collect();
    let non_certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .skip(one_fault as usize)
        .map(|authority| (authority, mixed_parents.clone()))
        .collect();
    references_at_certificate_round
        .extend(build_dag_layer(non_certifier_connections, &mut storage));

    // Build through to L2's decision round (round 8 for wave_length = 3).
    let decision_round_2 = 3 * WAVE_LENGTH - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_certificate_round),
        decision_round_2,
    );

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        build_protocol(&committee),
    );

    let last_committed: Option<(RoundNumber, Authority)> = None;
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 2);
    let leader_1 = leader_elector.elect_leader(leader_round_1);
    if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
        sequence[0]
    {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!(
            "Expected the first leader to be committed, got {:?}",
            sequence[0]
        );
    }
}

/// Commit L1, indirect-skip L2 (too few certifiers), commit L3.
///
/// This is the realistic Cordial Miners skip path: only `f+1` authorities link to L2 at
/// its voting round (the rest don't), so direct-skip's unanimous threshold is not
/// reached and L2 stays undecided directly. Because no round-(decision) block ends up
/// being a certificate for L2 (each sees only 2 of L2's would-be supporters among its
/// includes, below the strong-quorum certificate threshold), the eventual L3 anchor
/// finds nothing linked to L2 and indirect-skips it.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let protocol = build_protocol(&committee);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize; // = 3 for n=4.
    let supporters_count = committee.len() - blamers_count; // = 1.
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Build through L2's leader round.
    let leader_round_2 = 2 * WAVE_LENGTH;
    let references_at_leader_2 = build_dag(&committee, &mut storage, None, leader_round_2);
    let skipped_leader = leader_elector.elect_leader(leader_round_2);
    let references_without_leader_2: Vec<_> = references_at_leader_2
        .iter()
        .copied()
        .filter(|reference| reference.authority != skipped_leader)
        .collect();

    // Voting layer for L2: uniform `direct_skip_quorum - 1` blamers (so direct-skip
    // just barely fails), the rest support. With 1 vote among the 4 round-(voting)
    // references, no round-(decision) block reaches `strong_quorum` votes in its
    // includes — so no certificates exist for L2 and the eventual L3 anchor's
    // aggregator stays at zero → indirect_skip.
    let mut references_at_voting_round = Vec::new();
    let with_leader_2: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .take(supporters_count)
        .map(|authority| (authority, references_at_leader_2.clone()))
        .collect();
    references_at_voting_round.extend(build_dag_layer(with_leader_2, &mut storage));
    let without_leader_2: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .skip(supporters_count)
        .map(|authority| (authority, references_without_leader_2.clone()))
        .collect();
    references_at_voting_round.extend(build_dag_layer(without_leader_2, &mut storage));

    let decision_round_3 = 4 * WAVE_LENGTH - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_voting_round),
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

    assert_eq!(sequence.len(), 3);

    // L1 (committed).
    let leader_1 = leader_elector.elect_leader(WAVE_LENGTH);
    if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
        sequence[0]
    {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!("Expected L1 to be committed, got {:?}", sequence[0]);
    }

    // L2 (skipped).
    if let LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round) =
        sequence[1]
    {
        assert_eq!(leader, skipped_leader);
        assert_eq!(round, leader_round_2);
    } else {
        panic!("Expected L2 to be skipped, got {:?}", sequence[1]);
    }

    // L3 (committed).
    let leader_3 = leader_elector.elect_leader(3 * WAVE_LENGTH);
    if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
        sequence[2]
    {
        assert_eq!(block.author(), leader_3);
    } else {
        panic!("Expected L3 to be committed, got {:?}", sequence[2]);
    }
}

/// One supporter at the voting round + (strong_quorum-1) non-supporters yields neither
/// commit nor skip thresholds — L1 is undecided after the direct phase. The DAG depth
/// here doesn't reach a later anchor.
#[test]
#[tracing_test::traced_test]
fn undecided() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = WAVE_LENGTH;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let mut authorities = committee.authorities();
    let leader_connection = vec![(authorities.next().unwrap(), references_at_leader_round)];
    let non_leader_connections: Vec<_> = authorities
        .take((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let connections = leader_connection.into_iter().chain(non_leader_connections);
    let references_at_voting_round = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = 2 * WAVE_LENGTH - 1;
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

    let leader_round_1 = WAVE_LENGTH;
    let references_at_leader_round = build_dag(&committee, &mut storage, None, leader_round_1);
    let references_without_leader_1: Vec<_> = references_at_leader_round
        .into_iter()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();
    let decision_round_1 = 2 * WAVE_LENGTH - 1;
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
