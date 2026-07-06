// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `equivocation_certificate_beats_weak_quorum` scenario across the fast-path
//! protocol matrix.
//!
//! An equivocating target leader produces two blocks: B1 gathers exactly
//! `certificate_quorum` votes and `direct_commit_quorum - 1` certificates (one
//! short of a slow commit, so the direct rule stays undecided), while B2
//! gathers exactly `weak_indirect_quorum` votes. The anchor links everything,
//! so both rungs of the graded indirect rule have a qualifier — the test pins
//! that the certificate rung is evaluated first and B1 wins, even though B2
//! clears the weak quorum.
//!
//! Requires disjoint voter sets (`certificate_quorum + weak_indirect_quorum
//! <= n`), which also guarantees the B1 votes stay below the fast quorum;
//! configurations violating it are skipped (quorum intersection makes the two
//! footprints mutually exclusive there — a protocol property, not a test gap).

use std::sync::Arc;

use consensus::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    block::Block,
    committee::Committee,
    consensus::LeaderStatus,
    crypto::{BLOCK_DIGEST_SIZE, BlockDigest},
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee, insert_test_block},
};

#[test]
#[tracing_test::traced_test]
fn equivocation_certificate_beats_weak_quorum_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn equivocation_certificate_beats_weak_quorum_n20() {
    run_for_size(20);
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    let leader_counts = [1, 2, 2 * n / 3 + 1, n];
    for spec in ConsensusProtocol::all_fast_path_for_test(n, &leader_counts) {
        run(&spec, &committee);
    }
}

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let fast_path = protocol.fast_path.expect("fast-path protocol");
    let b1_voters = protocol.certificate_quorum as usize;
    let b2_voters = fast_path.weak_indirect_quorum as usize;
    if b1_voters + b2_voters > committee.len() {
        return;
    }
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len());

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let decision_round = committer.decision_round_for(l1);
        let target_leader = elector.elect_leader(l1 + target_offset as u64);
        if target_leader == Authority::default() {
            // The block store enforces that its own authority never equivocates
            // (one own block per round); observe equivocation from a peer, as a
            // correct replica would.
            continue;
        }

        let references_pre_leader = build_dag(committee, &mut storage, None, l1 - 1);

        // Leader layer: the target equivocates. B1 is its regular proposal; B2
        // carries a distinct digest (the synthetic test digest is keyed by round
        // and authority alone, so same-slot blocks would otherwise collapse into
        // one reference). Every other authority proposes normally.
        let leader_connections = committee
            .authorities()
            .map(|authority| (authority, references_pre_leader.clone()))
            .collect();
        let leader_refs = build_dag_layer(leader_connections, &mut storage);
        let b1_ref = *leader_refs
            .iter()
            .find(|reference| reference.authority == target_leader)
            .expect("target proposed in the leader layer");
        let honest_refs: Vec<_> = leader_refs
            .iter()
            .copied()
            .filter(|reference| reference.authority != target_leader)
            .collect();
        let mut b2_digest = [0u8; BLOCK_DIGEST_SIZE];
        b2_digest[BLOCK_DIGEST_SIZE - 1] = 1;
        let b2_ref = insert_test_block(
            Block::new_for_test(target_leader, l1, references_pre_leader.clone())
                .with_digest(BlockDigest::from(b2_digest)),
            &mut storage,
        );

        // Voting layer: `certificate_quorum` authorities vote B1,
        // `weak_indirect_quorum` vote B2, the rest blame the target slot.
        let voting_connections = committee
            .authorities()
            .enumerate()
            .map(|(i, authority)| {
                let mut parents = honest_refs.clone();
                if i < b1_voters {
                    parents.push(b1_ref);
                } else if i < b1_voters + b2_voters {
                    parents.push(b2_ref);
                }
                (authority, parents)
            })
            .collect();
        let voting_refs = build_dag_layer(voting_connections, &mut storage);
        let b1_vote_refs: Vec<_> = voting_refs[..b1_voters].to_vec();
        let other_vote_refs: Vec<_> = voting_refs[b1_voters..].to_vec();

        // Decision layer: `direct_commit_quorum - 1` certifiers link every B1
        // vote (each is a certificate for B1, one short of a slow commit); the
        // rest link the B2 votes and blames (below the certificate quorum, so
        // B2 is never certified and certificate uniqueness holds).
        let certifiers_count = (protocol.direct_commit_quorum - 1) as usize;
        let decision_connections = committee
            .authorities()
            .enumerate()
            .map(|(i, authority)| {
                let parents = if i < certifiers_count {
                    b1_vote_refs.clone()
                } else {
                    other_vote_refs.clone()
                };
                (authority, parents)
            })
            .collect();
        let decision_refs = build_dag_layer(decision_connections, &mut storage);

        // Forward DAG: the anchor commits and links both footprints.
        let anchor_round = committer.next_leader_round_after(decision_round);
        let anchor_decision = committer.decision_round_for(anchor_round);
        build_dag(
            committee,
            &mut storage,
            Some(decision_refs),
            anchor_decision,
        );

        let sequence = committer.try_commit(None).collect::<Vec<_>>();
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        assert!(
            sequence.len() >= k,
            "[{spec}] target_offset={target_offset} expected at least {k} decisions"
        );
        for (offset, decision) in sequence.iter().take(k).enumerate() {
            let expected = elector.elect_leader(l1 + offset as u64);
            if offset == target_offset {
                match decision {
                    LeaderStatus::IndirectCommit(block) => {
                        assert_eq!(
                            *block.reference(),
                            b1_ref,
                            "[{spec}] target_offset={target_offset} expected the certified \
                            B1, not the weak-quorum B2 ({b2_ref:?})"
                        );
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected IndirectCommit at \
                        offset {offset}, got {other:?}"
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
                        "[{spec}] target_offset={target_offset} expected DirectCommit at \
                        offset {offset}, got {other:?}"
                    ),
                }
            }
        }
    }
}
