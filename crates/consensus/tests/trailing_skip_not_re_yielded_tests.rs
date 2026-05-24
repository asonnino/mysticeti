// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `trailing_skip_not_re_yielded` scenario across the protocol matrix.
//!
//! Skips the last cohort leader at L1 so the final yielded decision is a Skip;
//! re-seeding with that Skip must yield nothing.

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
fn trailing_skip_not_re_yielded_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded_n10() {
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
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());
    let l1 = committer.nth_leader_round(1);
    let target_offset = k - 1;
    let target_leader = elector.elect_leader(l1 + target_offset as u64);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_target = drop_leader(&refs_at_leader, target_leader);
    let decision = committer.decision_round_for(l1);
    build_dag(committee, &mut storage, Some(refs_without_target), decision);

    let first = committer.try_commit(None).collect::<Vec<_>>();
    assert!(
        matches!(
            first.last(),
            Some(LeaderStatus::DirectSkip(..) | LeaderStatus::IndirectSkip(..)),
        ),
        "[{spec}] precondition: last decision must be a Skip, got {first:?}"
    );

    let seed = first
        .last()
        .map(|status| (status.round(), status.authority()));
    let second = committer.try_commit(seed).collect::<Vec<_>>();
    assert!(
        second.is_empty(),
        "[{spec}] trailing skip re-yielded: {second:?}"
    );
}
