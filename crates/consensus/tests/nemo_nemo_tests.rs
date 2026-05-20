// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Nemo-Nemo integration tests.

use std::num::NonZeroUsize;

use consensus::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee},
};

const WAVE_LENGTH: u64 = 2;
const COMMITTEE_SIZE: usize = 4;

fn build_protocol(committee: &Committee) -> Protocol {
    Protocol::nemo_nemo(committee.total_stake(), NonZeroUsize::new(1).unwrap())
}

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
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

/// Commit 10 leaders in a row.
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

/// Skip the first leader when it never proposes a block. With Nemo-Nemo's unanimous
/// skip threshold, every honest replica must produce a vote that omits the missing
/// leader — which happens naturally when the leader's round-1 block is absent.
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
        panic!("Expected to skip the leader");
    }
}

/// Directly skip the first leader when its round-1 block exists but no round-2 block
/// references it. With Nemo-Nemo's `direct_skip_quorum = n`, every authority's round-2
/// block must omit the leader — which is what `build_dag(.., Some(references_without_leader_1), 2)`
/// produces (all 4 round-2 blocks share the same leader-less parent set).
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
        panic!("Expected to skip the leader");
    }
}

/// Indirect-commit the first leader via a later anchor.
///
/// With Nemo-Nemo's `anchor_link_size = 1`, even a single supporter at the leader's
/// decision round is enough for an indirect commit. Construction: 2 voters and 2
/// non-voters at round 2 — neither commit (2 < 3) nor skip (2 < 4) triggers
/// directly; the anchor at a later wave finds the supporter and indirect-commits.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = total / 2 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_at_round_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    let references_without_leader_1: Vec<_> = references_at_round_1
        .iter()
        .cloned()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    let supporters: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .take((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_at_round_1.clone()))
        .collect();
    let non_supporters: Vec<(Authority, Vec<BlockReference>)> = committee
        .authorities()
        .skip((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let connections_round_2: Vec<_> = supporters.into_iter().chain(non_supporters).collect();
    let references_at_round_2 = build_dag_layer(connections_round_2, &mut storage);

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

    let leader_1 = leader_elector.elect_leader(leader_round_1);
    let first = &sequence[0];
    if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = first {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!("Expected the first leader to be committed, got {first:?}");
    }
}

/// Indirect-skip a later leader by hiding all of its supporters from the eventual anchor.
///
/// With Nemo-Nemo's `anchor_link_size = 1` and unanimous direct-skip, the only way to
/// land at an *indirect* skip is to have one supporter at the leader's decision round
/// (so direct-skip fails) but to keep that supporter outside the anchor's causal past.
/// We do that by filtering the supporter's reference out of every block built at the
/// rounds after the split.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    // Build to the leader we want to skip. We target the second BaseCommitter-B wave —
    // round 3 with wave_length = 2 — so the indirect path runs through a later wave.
    let leader_round_to_skip: RoundNumber = WAVE_LENGTH + 1;
    let references_at_skip_round = build_dag(&committee, &mut storage, None, leader_round_to_skip);
    let skipped_leader = leader_elector.elect_leader(leader_round_to_skip);
    let references_without_skipped_leader: Vec<_> = references_at_skip_round
        .iter()
        .copied()
        .filter(|reference| reference.authority != skipped_leader)
        .collect();

    // Next round (the decision round of the skipped leader): one supporter, the rest
    // don't link to the skipped leader. With Nemo-Nemo's direct_skip_quorum = n, the
    // 3 non-supporters are NOT enough to direct-skip; the leader stays undecided.
    let mut authorities = committee.authorities();
    let supporter = authorities.next().unwrap();
    let supporter_connection = vec![(supporter, references_at_skip_round.clone())];
    let non_supporter_connections: Vec<(Authority, Vec<BlockReference>)> = authorities
        .map(|authority| (authority, references_without_skipped_leader.clone()))
        .collect();
    let connections_next_round: Vec<_> = supporter_connection
        .into_iter()
        .chain(non_supporter_connections)
        .collect();
    let references_next_round = build_dag_layer(connections_next_round, &mut storage);

    // Hide the supporter from later rounds: continue the DAG with references that
    // exclude the supporter's block. Now any later anchor's causal past misses the
    // supporter and the aggregator stays at zero → indirect-skip.
    let references_next_round_without_supporter: Vec<_> = references_next_round
        .into_iter()
        .filter(|reference| reference.authority != supporter)
        .collect();
    let decision_round_final = 3 * WAVE_LENGTH;
    build_dag(
        &committee,
        &mut storage,
        Some(references_next_round_without_supporter),
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

/// One voter at round 2 is enough to avoid direct-commit (< 3 quorum) AND direct-skip
/// (3 blamers < 4 skip quorum), keeping the leader undecided at the direct phase. The
/// DAG depth here doesn't reach a later anchor, so the leader stays undecided overall.
#[test]
#[tracing_test::traced_test]
fn undecided() {
    let committee = committee(COMMITTEE_SIZE);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = total / 2 + 1;
    let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);

    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    let references_1_without_leader: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|reference| reference.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

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
