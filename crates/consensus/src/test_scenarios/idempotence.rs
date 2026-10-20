// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `idempotence` scenario across the protocol matrix.

use std::sync::Arc;

use dag::{committee::Committee, storage::Storage, test_util::build_dag};

use crate::{committer::Committer, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let l2 = committer.nth_leader_round(2);
    let dag_depth = committer.earliest_decision_round_for(l2);
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
