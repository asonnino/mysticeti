// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `indirect_commit` scenario across the protocol matrix.
//!
//! Construction varies on `wave_length`:
//! - `wl == 2`: a single layer at the (collapsed) voting/decision round with
//!   `(direct_commit_quorum - 1)` voters and the rest non-voters. That's enough
//!   for the later anchor to reach `anchor_link_size` certificate paths while
//!   keeping direct commit below threshold.
//! - `wl >= 3`: `build_split_chain` to the voting round with all
//!   `direct_commit_quorum` authorities voting (final blamers = `n - direct_commit_quorum`),
//!   then a hand-crafted decision-round layer where only
//!   `(direct_commit_quorum - 1)` certifiers fully link to the voters.

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    block::BlockReference,
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, build_split_chain, committee, drop_leader},
};

#[test]
#[tracing_test::traced_test]
fn indirect_commit_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit_n10() {
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
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let l1 = committer.next_leader_round_after(0);
    let leader = LeaderElector::new(committee.len()).elect_leader(l1);

    let refs_at_leader = build_dag(committee, &mut storage, None, l1);
    let refs_without_leader = drop_leader(&refs_at_leader, leader);
    let voting_round = committer.voting_round_for(l1);
    let decision_round = committer.decision_round_for(l1);

    let top_refs: Vec<BlockReference> = if protocol.wave_length == 2 {
        let voters_count = (protocol.direct_commit_quorum - 1) as usize;
        let blamers_count = committee.len() - voters_count;
        let (supports, blames) = build_split_chain(
            committee,
            &mut storage,
            refs_at_leader,
            refs_without_leader,
            voting_round,
            blamers_count,
        );
        supports.into_iter().chain(blames).collect()
    } else {
        let blamers_count = committee.len() - protocol.direct_commit_quorum as usize;
        let (supports, blames) = build_split_chain(
            committee,
            &mut storage,
            refs_at_leader,
            refs_without_leader,
            voting_round,
            blamers_count,
        );
        let certifiers_count = (protocol.direct_commit_quorum - 1) as usize;
        let mut decision_refs = Vec::new();
        let certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .take(certifiers_count)
            .map(|authority| (authority, supports.clone()))
            .collect();
        decision_refs.extend(build_dag_layer(certifier_connections, &mut storage));

        let mixed_parents: Vec<_> = blames
            .into_iter()
            .chain(supports)
            .take(protocol.direct_commit_quorum as usize)
            .collect();
        let non_certifier_connections: Vec<(Authority, Vec<BlockReference>)> = committee
            .authorities()
            .skip(certifiers_count)
            .map(|authority| (authority, mixed_parents.clone()))
            .collect();
        decision_refs.extend(build_dag_layer(non_certifier_connections, &mut storage));
        decision_refs
    };

    let next_leader = committer.next_leader_round_after(decision_round);
    let anchor_decision = committer.decision_round_for(next_leader);
    build_dag(committee, &mut storage, Some(top_refs), anchor_decision);

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    tracing::info!("[{spec}] Commit sequence: {sequence:?}");

    let first = sequence
        .first()
        .unwrap_or_else(|| panic!("[{spec}] expected at least one decision"));
    match first {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            assert_eq!(block.author(), leader, "[{spec}]");
        }
        other => panic!("[{spec}] expected L1 to be committed, got {other:?}"),
    }
}
