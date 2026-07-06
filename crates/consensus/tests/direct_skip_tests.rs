// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `direct_skip` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
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
fn direct_skip_n20() {
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
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let target_leader = elector.elect_leader(l1 + target_offset as u64);

        let l1_votes = build_dag(committee, &mut storage, None, l1);
        let l1_blames = drop_leader(&l1_votes, target_leader);
        let earliest_decision = committer.earliest_decision_round_for(l1);
        build_dag(committee, &mut storage, Some(l1_blames), earliest_decision);

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");
        assert_eq!(
            sequence.len(),
            k,
            "[{spec}] target_offset={target_offset} expected {k} decisions"
        );

        for (offset, decision) in sequence.iter().enumerate() {
            let expected = elector.elect_leader(l1 + offset as u64);
            if offset == target_offset {
                match decision {
                    LeaderStatus::DirectSkip(actual, round) => {
                        assert_eq!(*actual, expected, "[{spec}] target_offset={target_offset}");
                        assert_eq!(*round, l1, "[{spec}] target_offset={target_offset}");
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected skip at offset \
                        {offset}, got {other:?}"
                    ),
                }
            } else {
                match decision {
                    LeaderStatus::DirectCommit(block) => {
                        assert_eq!(
                            block.author(),
                            expected,
                            "[{spec}] target_offset={target_offset} offset={offset}"
                        );
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected commit at offset \
                        {offset}, got {other:?}"
                    ),
                }
            }
        }
    }
}
