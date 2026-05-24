// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `undecided` scenario across the protocol matrix.
//!
//! L1 cohort's first leader (offset 0) is undecided after the direct phase, and
//! the DAG doesn't extend far enough for a later anchor to resolve it. The
//! committer's `take_while` over decided statuses sees the undecided leader at
//! the front of the cohort and truncates the entire sequence.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    committee::Committee,
    storage::Storage,
    test_util::{build_dag, build_split_chain, committee, drop_leader},
};

#[test]
#[tracing_test::traced_test]
fn undecided_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn undecided_n10() {
    run_for_size(10);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    let leader_counts = [1, 2, 2 * n / 3 + 1, n];
    for spec in ConsensusProtocol::all_for_test(&leader_counts) {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let elector = LeaderElector::new(committee.len());
    let l1 = committer.next_leader_round_after(0);
    let target_offset = 0;
    let target_leader = elector.elect_leader(l1 + target_offset as u64);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_target = drop_leader(&refs_at_leader, target_leader);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
    let (supports, blames) = build_split_chain(
        committee,
        &mut storage,
        refs_at_leader,
        refs_without_target,
        committer.voting_round_for(l1),
        blamers_count,
    );
    let voting_refs: Vec<_> = supports.into_iter().chain(blames).collect();
    build_dag(
        committee,
        &mut storage,
        Some(voting_refs),
        committer.decision_round_for(l1),
    );

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");
    assert!(
        sequence.is_empty(),
        "[{spec}] expected empty sequence, got {sequence:?}"
    );
}
