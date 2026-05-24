// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `direct_commit` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee},
};

#[test]
#[tracing_test::traced_test]
fn direct_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_n10() {
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
    let l1 = committer.next_leader_round_after(0);
    let decision = committer.decision_round_for(l1);
    build_dag(committee, &mut storage, None, decision);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1, "[{spec}] expected 1 decision");
    let leader = LeaderElector::new(committee.len()).elect_leader(l1);
    match &sequence[0] {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            assert_eq!(block.author(), leader, "[{spec}]");
        }
        other => panic!("[{spec}] expected a committed leader, got {other:?}"),
    }
}
