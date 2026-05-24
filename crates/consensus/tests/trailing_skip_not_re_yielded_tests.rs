// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `trailing_skip_not_re_yielded` scenario across the protocol matrix.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
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
    for spec in ConsensusProtocol::all_for_test() {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(Authority::from(0u64), committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let l1 = committer.next_leader_round_after(0);
    let leader = LeaderElector::new(committee.len()).elect_leader(l1);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let decision = committer.decision_round_for(l1);
    build_dag(committee, &mut storage, Some(refs_without_leader), decision);

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
