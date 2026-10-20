// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `direct_commit_partial_round` scenario across the protocol matrix.
//!
//! After the first cohort leader at L1 is treated as already-committed,
//! calling `try_commit` again with that leader as the seed must yield the
//! remaining `K-1` cohort members in offset order. Exercises mid-cohort
//! resumption — a code path not covered by the other scenarios, which either
//! seed at a cohort boundary (`multiple_direct_commit`) or seed with the
//! *last* of a cohort and expect an empty sequence (`idempotence`).

use std::sync::Arc;

use dag::{committee::Committee, consensus::LeaderStatus, storage::Storage, test_util::build_dag};

use crate::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let l1 = committer.nth_leader_round(1);
    let earliest_decision = committer.earliest_decision_round_for(l1);
    build_dag(committee, &mut storage, None, earliest_decision);

    let elector = LeaderElector::new(committee.len(), protocol.leader_election);
    let first_leader = elector.elect_leader(l1, 0);
    let seed = Some((l1, first_leader));

    let sequence = committer.try_commit(seed).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");

    assert_eq!(
        sequence.len(),
        k - 1,
        "[{spec}] expected {} decisions",
        k - 1
    );
    for (i, decision) in sequence.iter().enumerate() {
        let offset = i + 1;
        let expected = elector.elect_leader(l1, offset as u64);
        match decision {
            LeaderStatus::DirectCommit(block, _) => {
                assert_eq!(block.author(), expected, "[{spec}] offset={offset}");
            }
            other => panic!("[{spec}] offset={offset} expected commit, got {other:?}"),
        }
    }
}
