// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `direct_skip` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee, drop_leader},
};

#[test]
#[tracing_test::traced_test]
fn direct_skip_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn direct_skip_n10() {
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
    let leader = LeaderElector::new(committee.len()).elect_leader(l1);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let decision = committer.decision_round_for(l1);
    build_dag(committee, &mut storage, Some(refs_without_leader), decision);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 1, "[{spec}] expected 1 decision");
    match &sequence[0] {
        LeaderStatus::DirectSkip(actual_leader, round)
        | LeaderStatus::IndirectSkip(actual_leader, round) => {
            assert_eq!(*actual_leader, leader, "[{spec}]");
            assert_eq!(*round, l1, "[{spec}]");
        }
        other => panic!("[{spec}] expected to skip the leader, got {other:?}"),
    }
}
