// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared scaffolding for the per-protocol integration test suites.

use std::collections::HashSet;

use consensus::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::{Committee, Stake},
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, build_split_chain, committee},
};

/// Per-protocol knobs consumed by the generic runners.
pub trait Fixture {
    /// Committee size. Four is enough for every BFT-quorum protocol with
    /// `n = 3f + 1`; BlueBottle uses ten to give its `4n/5 + 1` quorums clean
    /// integer headroom.
    const COMMITTEE_SIZE: usize;

    /// Construct the protocol under test for the given committee.
    fn build_protocol(committee: &Committee) -> Protocol;
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

fn setup<F: Fixture>() -> (std::sync::Arc<Committee>, Storage, Committer) {
    let committee = committee(F::COMMITTEE_SIZE);
    let storage = Storage::new_for_test(Authority::from(0u64), &committee);
    let committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        F::build_protocol(&committee),
    );
    (committee, storage, committer)
}

fn drop_leader(refs: &[BlockReference], leader: Authority) -> Vec<BlockReference> {
    refs.iter()
        .copied()
        .filter(|reference| reference.authority != leader)
        .collect()
}

// ---------------------------------------------------------------------------
// Uniform runners (six happy-path + two simple-skip)
// ---------------------------------------------------------------------------

/// Commit one leader.
pub fn run_direct_commit<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let l1 = committer.next_leader_round_after(0);
    let decision = committer.decision_round_for(l1);
    build_dag(&committee, &mut storage, None, decision);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    let leader = elect_leader::<F>(&committee, l1);
    match &sequence[0] {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            assert_eq!(block.author(), leader);
        }
        other => panic!("Expected a committed leader, got {other:?}"),
    }
}

/// A second `try_commit` re-seeded with the last decision must yield nothing.
pub fn run_idempotence<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    // Build a few decisions worth of DAG so the first call returns something.
    let l1 = committer.next_leader_round_after(0);
    let dag_depth = committer.decision_round_for(committer.next_leader_round_after(l1));
    build_dag(&committee, &mut storage, None, dag_depth);

    let committed = committer.try_commit(None).collect::<Vec<_>>();
    assert!(!committed.is_empty(), "first try_commit returned nothing");
    let last = committed.into_iter().last().unwrap();
    let seed = Some((last.round(), last.authority()));
    let sequence = committer.try_commit(seed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

/// Commit each successive leader as the DAG grows by one decision round at a time.
pub fn run_multiple_direct_commit<F: Fixture>() {
    let committee = committee(F::COMMITTEE_SIZE);
    let mut last_committed: Option<(RoundNumber, Authority)> = None;

    let template_committer = Committer::new(
        committee.clone(),
        Storage::new_for_test(Authority::from(0u64), &committee)
            .block_reader()
            .clone(),
        F::build_protocol(&committee),
    );

    for n in 1..=10u64 {
        let leader_round = nth_leader_round(&template_committer, n);
        let dag_depth = template_committer.decision_round_for(leader_round);

        let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, dag_depth);
        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            F::build_protocol(&committee),
        );

        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        let leader = elect_leader::<F>(&committee, leader_round);
        match &sequence[0] {
            LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                assert_eq!(block.author(), leader);
            }
            other => panic!("Expected a committed leader, got {other:?}"),
        }
        let last = sequence.into_iter().last().unwrap();
        last_committed = Some((last.round(), last.authority()));
    }
}

/// Build the DAG to the 10th leader's decision round, then commit all 10 at once.
pub fn run_direct_commit_late_call<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let leader_rounds: Vec<RoundNumber> = (1..=10u64)
        .map(|n| nth_leader_round(&committer, n))
        .collect();
    let dag_depth = committer.decision_round_for(*leader_rounds.last().unwrap());
    build_dag(&committee, &mut storage, None, dag_depth);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 10);
    for (decision, &leader_round) in sequence.iter().zip(&leader_rounds) {
        let leader = elect_leader::<F>(&committee, leader_round);
        match decision {
            LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                assert_eq!(block.author(), leader);
            }
            other => panic!("Expected a committed leader, got {other:?}"),
        }
    }
}

/// Nothing commits before the first leader's decision round.
pub fn run_no_genesis_commit<F: Fixture>() {
    let committee = committee(F::COMMITTEE_SIZE);
    let template_committer = Committer::new(
        committee.clone(),
        Storage::new_for_test(Authority::from(0u64), &committee)
            .block_reader()
            .clone(),
        F::build_protocol(&committee),
    );
    let l1 = template_committer.next_leader_round_after(0);
    let first_commit_round = template_committer.decision_round_for(l1);

    for r in 0..first_commit_round {
        let mut storage = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, r);
        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            F::build_protocol(&committee),
        );
        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}

/// Skip the first leader when it never proposes a block. Every voting-round
/// block omits the leader, so the unanimous-or-quorum direct-skip threshold
/// fires regardless of protocol.
pub fn run_no_leader<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let l1 = committer.next_leader_round_after(0);
    let leader = elect_leader::<F>(&committee, l1);

    // Build whatever DAG sits below the leader round. For `l1 == 1` that's the
    // genesis layer; for `l1 > 1` we build through `l1 - 1`.
    let references_pre_leader: Vec<BlockReference> = if l1 == 1 {
        committee
            .authorities()
            .map(|authority| *Block::genesis(authority).reference())
            .collect()
    } else {
        build_dag(&committee, &mut storage, None, l1 - 1)
    };

    let connections = committee
        .authorities()
        .filter(|&a| a != leader)
        .map(|a| (a, references_pre_leader.clone()))
        .collect();
    let references_at_leader = build_dag_layer(connections, &mut storage);
    let decision = committer.decision_round_for(l1);
    build_dag(
        &committee,
        &mut storage,
        Some(references_at_leader),
        decision,
    );

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 1);
    match &sequence[0] {
        LeaderStatus::DirectSkip(actual_leader, round)
        | LeaderStatus::IndirectSkip(actual_leader, round) => {
            assert_eq!(*actual_leader, leader);
            assert_eq!(*round, l1);
        }
        other => panic!("Expected to skip the leader, got {other:?}"),
    }
}

/// Direct-skip the first leader when no voting-round block links to it.
pub fn run_direct_skip<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let l1 = committer.next_leader_round_after(0);
    let leader = elect_leader::<F>(&committee, l1);

    let refs_at_leader = build_dag(&committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let decision = committer.decision_round_for(l1);
    build_dag(
        &committee,
        &mut storage,
        Some(refs_without_leader),
        decision,
    );

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 1);
    match &sequence[0] {
        LeaderStatus::DirectSkip(actual_leader, round)
        | LeaderStatus::IndirectSkip(actual_leader, round) => {
            assert_eq!(*actual_leader, leader);
            assert_eq!(*round, l1);
        }
        other => panic!("Expected to skip the leader, got {other:?}"),
    }
}

/// A Skip yielded once must not be re-yielded when the committer is re-seeded.
pub fn run_trailing_skip_not_re_yielded<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let l1 = committer.next_leader_round_after(0);
    let leader = elect_leader::<F>(&committee, l1);

    let refs_at_leader = build_dag(&committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let decision = committer.decision_round_for(l1);
    build_dag(
        &committee,
        &mut storage,
        Some(refs_without_leader),
        decision,
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

// ---------------------------------------------------------------------------
// Variant runners
// ---------------------------------------------------------------------------

/// Indirect-commit L1 via a later anchor.
///
/// Construction varies on `wave_length`:
/// - `wl == 2`: a single layer at the (collapsed) voting/decision round with
///   `(direct_commit_quorum - 1)` voters and the rest non-voters. That's enough
///   for the later anchor to reach `anchor_link_size` certificate paths while
///   keeping direct commit below threshold.
/// - `wl >= 3`: `build_split_chain` to the voting round with all `direct_commit_quorum`
///   authorities voting (final blamers = `n - direct_commit_quorum`), then a
///   hand-crafted decision-round layer where only `(direct_commit_quorum - 1)`
///   certifiers fully link to the voters (so only `(direct_commit_quorum - 1)`
///   certificates exist — not enough for direct commit, but enough for a later
///   anchor to indirect-commit).
pub fn run_indirect_commit<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let protocol = F::build_protocol(&committee);
    let l1 = committer.next_leader_round_after(0);
    let leader = elect_leader::<F>(&committee, l1);

    let refs_at_leader = build_dag(&committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let voting_round = committer.voting_round_for(l1);
    let decision_round = committer.decision_round_for(l1);

    let top_refs: Vec<BlockReference> = if protocol.wave_length == 2 {
        // Single split layer at the collapsed voting/decision round.
        let voters_count = (protocol.direct_commit_quorum - 1) as usize;
        let blamers_count = F::COMMITTEE_SIZE - voters_count;
        let (supports, blames) = build_split_chain(
            &committee,
            &mut storage,
            refs_at_leader,
            refs_without_leader,
            voting_round,
            blamers_count,
        );
        supports.into_iter().chain(blames).collect()
    } else {
        // Chained split to the voting round with all `direct_commit_quorum` voters.
        let blamers_count = F::COMMITTEE_SIZE - protocol.direct_commit_quorum as usize;
        let (supports, blames) = build_split_chain(
            &committee,
            &mut storage,
            refs_at_leader,
            refs_without_leader,
            voting_round,
            blamers_count,
        );
        // Hand-crafted decision-round layer: `(direct_commit_quorum - 1)`
        // certifiers see only the voters; the rest see a mixed set with too few
        // votes to certify.
        let certifiers_count = (protocol.direct_commit_quorum - 1) as usize;
        let mut decision_refs = Vec::new();
        let certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .take(certifiers_count)
            .map(|authority| (authority, supports.clone()))
            .collect();
        decision_refs.extend(build_dag_layer(certifier_connections, &mut storage));

        let mixed_parents: Vec<_> = blames
            .into_iter()
            .chain(supports)
            .take(protocol.direct_commit_quorum as usize)
            .collect();
        let non_certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .skip(certifiers_count)
            .map(|authority| (authority, mixed_parents.clone()))
            .collect();
        decision_refs.extend(build_dag_layer(non_certifier_connections, &mut storage));
        decision_refs
    };

    // Build forward to the next same-offset BaseCommitter's anchor decision.
    let next_leader = committer.next_leader_round_after(decision_round);
    let anchor_decision = committer.decision_round_for(next_leader);
    build_dag(&committee, &mut storage, Some(top_refs), anchor_decision);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    let first = sequence.first().expect("expected at least one decision");
    match first {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            assert_eq!(block.author(), leader);
        }
        other => panic!("expected L1 to be committed, got {other:?}"),
    }
}

/// Indirect-skip a target leader via a later anchor.
///
/// Two underlying constructions, auto-dispatched by [`use_hide_voters`]:
/// - **HideVoters** (today only Nemo-Nemo, `wl=2` with `anchor_link_size = 1`
///   and unanimous direct skip): a single split layer at the decision round
///   with the smallest valid number of voters, then the supporter refs are
///   excluded from the forward `build_dag` so the eventual anchor can't reach
///   them.
/// - **UniformBlamers** (everyone else): `build_split_chain` to the voting
///   round with `(direct_skip_quorum - 1)` blamers; the chain collapses to a
///   single layer when `wl <= 3`. The forward DAG is left full.
pub fn run_indirect_skip<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let protocol = F::build_protocol(&committee);
    // Target a leader one wave_length in so the indirect path runs through a
    // later wave of the same BaseCommitter.
    let l1 = committer.next_leader_round_after(0);
    let target_round = l1 + protocol.wave_length;
    let target_leader = elect_leader::<F>(&committee, target_round);
    let refs_at_target = build_dag(&committee, &mut storage, None, target_round);
    let refs_without_target = drop_leader(&refs_at_target, target_leader);

    let anchor_decision = committer.decision_round_for(
        committer.next_leader_round_after(committer.decision_round_for(target_round)),
    );

    if use_hide_voters(&protocol, F::COMMITTEE_SIZE) {
        // Smallest valid voters count: enough to dodge direct-skip
        // (`n - voters < direct_skip_quorum`) and below direct-commit
        // (`voters < direct_commit_quorum`).
        let voters_count = (F::COMMITTEE_SIZE as Stake - protocol.direct_skip_quorum + 1) as usize;
        let voters: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .take(voters_count)
            .map(|authority| (authority, refs_at_target.clone()))
            .collect();
        let non_voters: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .skip(voters_count)
            .map(|authority| (authority, refs_without_target.clone()))
            .collect();
        let refs_at_decision =
            build_dag_layer(voters.into_iter().chain(non_voters).collect(), &mut storage);
        let voter_authors: HashSet<_> = committee.authorities().take(voters_count).collect();
        let forward_refs: Vec<_> = refs_at_decision
            .into_iter()
            .filter(|reference| !voter_authors.contains(&reference.authority))
            .collect();
        build_dag(
            &committee,
            &mut storage,
            Some(forward_refs),
            anchor_decision,
        );
    } else {
        let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
        let (supports, blames) = build_split_chain(
            &committee,
            &mut storage,
            refs_at_target,
            refs_without_target,
            committer.voting_round_for(target_round),
            blamers_count,
        );
        let forward_refs: Vec<_> = supports.into_iter().chain(blames).collect();
        build_dag(
            &committee,
            &mut storage,
            Some(forward_refs),
            anchor_decision,
        );
    }

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    let skip_found = sequence.iter().any(|status| {
        matches!(
            status,
            LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round)
                if *leader == target_leader && *round == target_round
        )
    });
    assert!(
        skip_found,
        "expected Skip(leader={target_leader:?}, round={target_round}), got {sequence:?}",
    );
}

/// L1 is undecided after the direct phase, and the DAG doesn't extend far
/// enough for a later anchor to resolve it.
///
/// Uniform construction: `build_split_chain` to the voting round with
/// `(n - direct_skip_quorum + 1)` supporters and `(direct_skip_quorum - 1)`
/// blamers (smallest valid choice that satisfies both `voters < direct_commit_quorum`
/// and `blamers < direct_skip_quorum` across all protocols in scope). The
/// forward DAG stops at the leader's decision round.
pub fn run_undecided<F: Fixture>() {
    let (committee, mut storage, mut committer) = setup::<F>();
    let protocol = F::build_protocol(&committee);
    let l1 = committer.next_leader_round_after(0);
    let leader = elect_leader::<F>(&committee, l1);

    let refs_at_leader = build_dag(&committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
    let (supports, blames) = build_split_chain(
        &committee,
        &mut storage,
        refs_at_leader,
        refs_without_leader,
        committer.voting_round_for(l1),
        blamers_count,
    );
    let voting_refs: Vec<_> = supports.into_iter().chain(blames).collect();
    build_dag(
        &committee,
        &mut storage,
        Some(voting_refs),
        committer.decision_round_for(l1),
    );

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

/// Predicate selecting the "hide voters" indirect-skip construction.
///
/// The uniform `(direct_skip_quorum - 1)`-blamers pattern fails to yield an
/// indirect skip exactly when the residual supporter count (`n - direct_skip_quorum + 1`)
/// already meets `anchor_link_size` at `wl == 2`. In that case the later anchor
/// reaches enough certificate paths to indirect-*commit* the target, so the
/// test must instead actively hide the supporter blocks from the forward DAG.
/// Today this only fires for Nemo-Nemo.
fn use_hide_voters(protocol: &Protocol, committee_size: usize) -> bool {
    protocol.wave_length == 2
        && (committee_size as Stake - protocol.direct_skip_quorum + 1) >= protocol.anchor_link_size
}

// ---------------------------------------------------------------------------
// Local utilities
// ---------------------------------------------------------------------------

fn elect_leader<F: Fixture>(committee: &Committee, round: RoundNumber) -> Authority {
    LeaderElector::new(committee.len()).elect_leader(round)
}

/// Walk leader rounds starting from 0 and return the `n`-th one (1-indexed).
fn nth_leader_round(committer: &Committer, n: u64) -> RoundNumber {
    let mut round = 0;
    for _ in 0..n {
        round = committer.next_leader_round_after(round);
    }
    round
}
