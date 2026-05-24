// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `indirect_skip` scenario across the protocol matrix.
//!
//! Two underlying constructions, auto-dispatched by a predicate over protocol
//! fields:
//! - **HideVoters** (`wl=2` with `(n - direct_skip_quorum + 1) >= anchor_link_size`):
//!   a single split layer at the decision round with the smallest valid number
//!   of voters, then the supporter refs are excluded from the forward
//!   `build_dag` so the eventual anchor can't reach them. Needed because the
//!   residual supporter count already meets `anchor_link_size`, so the
//!   uniform-blamers construction would yield an indirect-*commit* instead.
//! - **UniformBlamers** (everyone else): `build_split_chain` to the voting
//!   round with `(direct_skip_quorum - 1)` blamers; the chain collapses to a
//!   single layer when `wl <= 3`. The forward DAG is left full.

use std::{collections::HashSet, sync::Arc};

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    block::BlockReference,
    committee::{Committee, Stake},
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, build_split_chain, committee, drop_leader},
};

#[test]
#[tracing_test::traced_test]
fn indirect_skip_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip_n10() {
    run_for_size(10);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    for spec in ConsensusProtocol::all_for_test() {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(Authority::from(0u64), committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let l1 = committer.next_leader_round_after(0);
    let target_round = l1 + protocol.wave_length;
    let target_leader = LeaderElector::new(committee.len()).elect_leader(target_round);
    let refs_at_target = build_dag(committee, &mut storage, None, target_round);
    let refs_without_target = drop_leader(&refs_at_target, target_leader);

    let anchor_decision = committer.decision_round_for(
        committer.next_leader_round_after(committer.decision_round_for(target_round)),
    );

    let use_hide_voters = protocol.wave_length == 2
        && (committee.len() as Stake - protocol.direct_skip_quorum + 1)
            >= protocol.anchor_link_size;
    if use_hide_voters {
        let voters_count = (committee.len() as Stake - protocol.direct_skip_quorum + 1) as usize;
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
        build_dag(committee, &mut storage, Some(forward_refs), anchor_decision);
    } else {
        let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
        let (supports, blames) = build_split_chain(
            committee,
            &mut storage,
            refs_at_target,
            refs_without_target,
            committer.voting_round_for(target_round),
            blamers_count,
        );
        let forward_refs: Vec<_> = supports.into_iter().chain(blames).collect();
        build_dag(committee, &mut storage, Some(forward_refs), anchor_decision);
    }

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");

    let skip_found = sequence.iter().any(|status| {
        matches!(
            status,
            LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round)
                if *leader == target_leader && *round == target_round
        )
    });
    assert!(
        skip_found,
        "[{spec}] expected Skip(leader={target_leader:?}, round={target_round}), got {sequence:?}"
    );
}
