// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `multiple_direct_commit` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    block::RoundNumber,
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee},
};

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit_n20() {
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
    let mut last_committed: Option<(RoundNumber, Authority)> = None;

    let template_storage = Storage::new_for_test(committee);
    let template_committer = Committer::new_for_test(committee, &template_storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());

    for n in 1..=10 {
        let leader_round = template_committer.nth_leader_round(n);
        let dag_depth = template_committer.decision_round_for(leader_round);

        let mut storage = Storage::new_for_test(committee);
        build_dag(committee, &mut storage, None, dag_depth);
        let mut committer = Committer::new_for_test(committee, &storage, spec);

        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("[{spec}] Commit sequence at n={n}: {sequence:?}");

        assert_eq!(sequence.len(), k, "[{spec}] n={n} expected {k} decisions");
        for (offset, decision) in sequence.iter().enumerate() {
            let expected = elector.elect_leader(leader_round + offset as u64);
            match decision {
                LeaderStatus::DirectCommit(block) => {
                    assert_eq!(block.author(), expected, "[{spec}] n={n} offset={offset}");
                }
                other => panic!("[{spec}] n={n} offset={offset} expected commit, got {other:?}"),
            }
        }
        let last = sequence.into_iter().last().unwrap();
        last_committed = Some((last.round(), last.authority()));
    }
}
