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

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
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
fn indirect_skip_n20() {
    run_for_size(20);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    let leader_counts = [1, 2, 2 * n / 3 + 1, n];
    for spec in ConsensusProtocol::all_for_test(n, &leader_counts) {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let target_round = l1 + protocol.wave_length;
        let target_leader = elector.elect_leader(target_round + target_offset as u64);
        let l1_votes = build_dag(committee, &mut storage, None, target_round);
        let l1_blames = drop_leader(&l1_votes, target_leader);

        let anchor_decision = committer.decision_round_for(
            committer.next_leader_round_after(committer.decision_round_for(target_round)),
        );

        let use_hide_voters = protocol.wave_length == 2
            && (committee.len() as Stake - protocol.direct_skip_quorum + 1)
                >= protocol.anchor_link_size;
        if use_hide_voters {
            let voters_count =
                (committee.len() as Stake - protocol.direct_skip_quorum + 1) as usize;
            let connections = committee
                .authorities()
                .enumerate()
                .map(|(i, authority)| {
                    let parents = if i < voters_count {
                        l1_votes.clone()
                    } else {
                        l1_blames.clone()
                    };
                    (authority, parents)
                })
                .collect();
            let refs_at_decision = build_dag_layer(connections, &mut storage);
            let forward_refs: Vec<_> = refs_at_decision.into_iter().skip(voters_count).collect();
            build_dag(committee, &mut storage, Some(forward_refs), anchor_decision);
        } else {
            let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
            let (supports, blames) = build_split_chain(
                committee,
                &mut storage,
                l1_votes,
                l1_blames,
                committer.voting_round_for(target_round),
                blamers_count,
            );
            let forward_refs: Vec<_> = supports.into_iter().chain(blames).collect();
            build_dag(committee, &mut storage, Some(forward_refs), anchor_decision);
        }

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        for offset in 0..k {
            let leader = elector.elect_leader(target_round + offset as u64);
            if offset == target_offset {
                let expected = LeaderStatus::IndirectSkip(leader, target_round);
                assert!(
                    sequence.contains(&expected),
                    "[{spec}] target_offset={target_offset} missing {expected:?}, \
                    got {sequence:?}"
                );
            } else {
                let direct_committed = sequence.iter().any(|status| match status {
                    LeaderStatus::DirectCommit(block) => {
                        block.author() == leader && block.round() == target_round
                    }
                    _ => false,
                });
                assert!(
                    direct_committed,
                    "[{spec}] target_offset={target_offset} missing \
                    DirectCommit(leader={leader:?}, round={target_round}), got {sequence:?}"
                );
            }
        }
    }
}
