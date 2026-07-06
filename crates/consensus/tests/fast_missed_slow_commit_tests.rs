// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `fast_missed_slow_commit` scenario across the fast-path protocol matrix.
//!
//! The target leader misses the fast path by one vote (`commit_quorum - 1`
//! voters, the rest blame), but a fully-connected decision round turns every
//! decision block into a certificate: the slow path commits the target
//! directly. Skipped for configurations where the certificate quorum reaches
//! the fast commit quorum: there any certificate implies a fast commit, so
//! the slow path is unreachable by construction (a protocol property, not a
//! test gap).

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
fn fast_missed_slow_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn fast_missed_slow_commit_n20() {
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
    if protocol.certificate_quorum >= fast_path.commit_quorum {
        return;
    }
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

        // Voting layer: one vote short of the fast path; the certificate quorum
        // is still met (gated above).
        let voters_count = (fast_path.commit_quorum - 1) as usize;
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

        // Fully-connected decision round: every decision block references all
        // voters, so each is a certificate and the slow path commits.
        build_dag(committee, &mut storage, Some(voting_refs), decision_round);

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        assert!(
            sequence.len() >= k,
            "[{spec}] target_offset={target_offset} expected at least {k} decisions"
        );
        for (offset, decision) in sequence.iter().take(k).enumerate() {
            let expected = elector.elect_leader(l1 + offset as u64);
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
