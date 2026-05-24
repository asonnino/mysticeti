// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `direct_commit_late_call` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    block::RoundNumber,
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee},
};

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call_n10() {
    run_for_size(10);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    let leader_counts = [1, 2, 2 * n / 3 + 1, n];
    for spec in ConsensusProtocol::all_for_test(&leader_counts) {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let leader_rounds: Vec<RoundNumber> =
        (1..=10u64).map(|n| committer.nth_leader_round(n)).collect();
    let dag_depth = committer.decision_round_for(*leader_rounds.last().unwrap());
    build_dag(committee, &mut storage, None, dag_depth);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");

    assert_eq!(
        sequence.len(),
        10 * k,
        "[{spec}] expected {} decisions",
        10 * k
    );
    let elector = LeaderElector::new(committee.len());
    for (chunk_idx, chunk) in sequence.chunks(k).enumerate() {
        let leader_round = leader_rounds[chunk_idx];
        for (offset, decision) in chunk.iter().enumerate() {
            let expected = elector.elect_leader(leader_round + offset as u64);
            match decision {
                LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                    assert_eq!(
                        block.author(),
                        expected,
                        "[{spec}] chunk={chunk_idx} offset={offset}"
                    );
                }
                other => panic!(
                    "[{spec}] chunk={chunk_idx} offset={offset} expected commit, got {other:?}"
                ),
            }
        }
    }
}
