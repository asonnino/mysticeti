// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `no_genesis_commit` scenario across the protocol matrix.

use std::sync::Arc;

use dag::{committee::Committee, storage::Storage, test_util::build_dag};

use crate::{committer::Committer, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let template_storage = Storage::new_for_test(committee);
    let template_committer = Committer::new_for_test(committee, &template_storage, spec);
    let l1 = template_committer.nth_leader_round(1);
    let earliest_decision = template_committer.earliest_decision_round_for(l1);

    for r in 0..earliest_decision {
        let mut storage = Storage::new_for_test(committee);
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
