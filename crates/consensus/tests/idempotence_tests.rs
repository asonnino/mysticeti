// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `idempotence` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, protocol::ConsensusProtocol};
use dag::{
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
fn idempotence_n20() {
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
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let l2 = committer.nth_leader_round(2);
    let dag_depth = committer.decision_round_for(l2);
    build_dag(committee, &mut storage, None, dag_depth);

    let committed = committer.try_commit(None).collect::<Vec<_>>();
    assert_eq!(
        committed.len(),
        2 * k,
        "[{spec}] first try_commit expected {} decisions",
        2 * k
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
