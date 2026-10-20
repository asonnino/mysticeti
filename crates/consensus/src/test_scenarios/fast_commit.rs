// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `fast_commit` scenario across the fast-path protocol matrix.
//!
//! Builds votes at the voting round and nothing above it: the decision round
//! is absent, so no certificate can exist and a commit can only come from the
//! fast path. The target leader receives exactly `fast_path.commit_quorum`
//! votes (the boundary), the remaining authorities blame it; sibling cohort
//! leaders receive every vote.

use std::sync::Arc;

use dag::{
    committee::Committee,
    consensus::{DirectCommitPath, LeaderStatus},
    storage::Storage,
    test_util::{build_dag, build_dag_layer, drop_leader},
};

use crate::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let fast_path = protocol.fast_path.expect("fast-path protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len(), protocol.leader_election);

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let target_leader = elector.elect_leader(l1, target_offset as u64);

        let l1_votes = build_dag(committee, &mut storage, None, l1);
        let l1_blames = drop_leader(&l1_votes, target_leader);

        // Voting layer only: the first `commit_quorum` authorities vote for the
        // target, the rest blame it. No decision round exists, so the fast path
        // is the only way to commit.
        let voters_count = fast_path.commit_quorum as usize;
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
        build_dag_layer(connections, &mut storage);

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        assert_eq!(
            sequence.len(),
            k,
            "[{spec}] target_offset={target_offset} expected {k} decisions"
        );
        for (offset, decision) in sequence.iter().enumerate() {
            let expected = elector.elect_leader(l1, offset as u64);
            match decision {
                LeaderStatus::DirectCommit(block, path) => {
                    assert_eq!(
                        block.author(),
                        expected,
                        "[{spec}] target_offset={target_offset} offset={offset}"
                    );
                    assert_eq!(
                        *path,
                        DirectCommitPath::Fast,
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
