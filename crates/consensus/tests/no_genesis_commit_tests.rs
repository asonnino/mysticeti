// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `no_genesis_commit` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    committee::Committee,
    storage::Storage,
    test_util::{build_dag, committee},
};

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit_n10() {
    run_for_size(10);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    for spec in ConsensusProtocol::all_for_test() {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let template_storage = Storage::new_for_test(Authority::from(0u64), committee);
    let template_committer = Committer::new_for_test(committee, &template_storage, spec);
    let l1 = template_committer.next_leader_round_after(0);
    let first_commit_round = template_committer.decision_round_for(l1);

    for r in 0..first_commit_round {
        let mut storage = Storage::new_for_test(Authority::from(0u64), committee);
        build_dag(committee, &mut storage, None, r);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] Commit sequence at r={r}: {sequence:?}");
        assert!(
            sequence.is_empty(),
            "[{spec}] r={r} expected empty sequence, got {sequence:?}"
        );
    }
}
