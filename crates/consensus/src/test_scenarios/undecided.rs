// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `undecided` scenario across the protocol matrix.
//!
//! L1 cohort's first leader (offset 0) is undecided after the direct phase, and
//! the DAG doesn't extend far enough for a later anchor to resolve it. The
//! committer's `take_while` over decided statuses sees the undecided leader at
//! the front of the cohort and truncates the entire sequence.

use std::sync::Arc;

use dag::{
    committee::Committee,
    storage::Storage,
    test_util::{build_dag, build_partial_split_chain, drop_leader},
};

use crate::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let mut storage = Storage::new_for_test(committee);
    let mut committer = Committer::new_for_test(committee, &storage, spec);
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let elector = LeaderElector::new(committee.len());
    let l1 = committer.nth_leader_round(1);
    let target_offset = 0;
    let target_leader = elector.elect_leader(l1 + target_offset as u64);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_target = drop_leader(&refs_at_leader, target_leader);
    let blamers_count = (protocol.direct_skip_quorum - 1) as usize;
    // When skip + cert = n + 1 (Hybrid Mysticeti at f = 0), a complete voting round always
    // decides the leader; leave voters out so the supporters stay one short of a certificate.
    let supporters_count =
        (committee.len() - blamers_count).min((protocol.certificate_quorum - 1) as usize);
    let (supports, blames) = build_partial_split_chain(
        committee,
        &mut storage,
        refs_at_leader,
        refs_without_target,
        committer.voting_round_for(l1),
        supporters_count,
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
