// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `idempotence` scenario across the protocol matrix.

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
fn idempotence_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn idempotence_n10() {
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
    let dag_depth = committer.decision_round_for(committer.next_leader_round_after(l1));
    build_dag(committee, &mut storage, None, dag_depth);

    let committed = committer.try_commit(None).collect::<Vec<_>>();
    assert!(
        !committed.is_empty(),
        "[{spec}] first try_commit returned nothing"
    );
    let last = committed.into_iter().last().unwrap();
    let seed = Some((last.round(), last.authority()));
    let sequence = committer.try_commit(seed).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");
    assert!(
        sequence.is_empty(),
        "[{spec}] expected empty re-seeded sequence, got {sequence:?}"
    );
}
