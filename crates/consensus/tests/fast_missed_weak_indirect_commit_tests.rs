// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `fast_missed_weak_indirect_commit` scenario across the fast-path protocol
//! matrix.
//!
//! The target leader receives exactly `weak_indirect_quorum` votes: too few
//! for the fast path, too few for any certificate (the weak quorum is always
//! below both), and the blames stay below the skip quorum — the direct rule
//! leaves the target undecided. The forward DAG commits a later anchor that
//! links every vote, so the graded indirect rule finds no certificate (rung 1)
//! but a weak quorum of anchor-linked votes (rung 2) and indirect-commits the
//! target.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee, drop_leader},
};

#[test]
#[tracing_test::traced_test]
fn fast_missed_weak_indirect_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn fast_missed_weak_indirect_commit_n20() {
    run_for_size(20);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    let leader_counts = [1, 2, 2 * n / 3 + 1, n];
    for spec in ConsensusProtocol::all_fast_path_for_test(n, &leader_counts) {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let fast_path = protocol.fast_path.expect("fast-path protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let decision_round = committer.decision_round_for(l1);
        let target_leader = elector.elect_leader(l1 + target_offset as u64);

        let l1_votes = build_dag(committee, &mut storage, None, l1);
        let l1_blames = drop_leader(&l1_votes, target_leader);

        // Voting layer: exactly the weak indirect quorum votes for the target.
        let voters_count = fast_path.weak_indirect_quorum as usize;
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
        let voting_refs = build_dag_layer(connections, &mut storage);

        // Fully-connected forward DAG up to the anchor's decision round: no
        // decision block carries enough votes for a certificate, but the later
        // anchor links every vote.
        let anchor_round = committer.next_leader_round_after(decision_round);
        let anchor_decision = committer.decision_round_for(anchor_round);
        build_dag(committee, &mut storage, Some(voting_refs), anchor_decision);

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        assert!(
            sequence.len() >= k,
            "[{spec}] target_offset={target_offset} expected at least {k} decisions"
        );
        for (offset, decision) in sequence.iter().take(k).enumerate() {
            let expected = elector.elect_leader(l1 + offset as u64);
            if offset == target_offset {
                match decision {
                    LeaderStatus::IndirectCommit(block) => {
                        assert_eq!(
                            block.author(),
                            expected,
                            "[{spec}] target_offset={target_offset}"
                        );
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected IndirectCommit at \
                        offset {offset}, got {other:?}"
                    ),
                }
            } else {
                match decision {
                    LeaderStatus::DirectCommit(block) => {
                        assert_eq!(
                            block.author(),
                            expected,
                            "[{spec}] target_offset={target_offset} offset={offset}"
                        );
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected DirectCommit at \
                        offset {offset}, got {other:?}"
                    ),
                }
            }
        }
    }
}
