// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `indirect_commit` scenario across the protocol matrix.
//!
//! Construction varies on the certificate geometry:
//! - merged certificates (the `wl == 2` protocols, async Blue Bottle at
//!   `wl == 3`): a split chain to the
//!   (collapsed) voting/decision round with `(direct_commit_quorum - 1)` voters
//!   and the rest non-voters. That's enough for the later anchor to reach
//!   `anchor_link_size` certificate paths while keeping direct commit below
//!   threshold.
//! - vote-and-certify: `build_split_chain` to the voting round with enough voters to
//!   back a certificate (`certificate_quorum`, capped one below the fast-path
//!   commit quorum so fast-path protocols stay undecided at the voting round),
//!   then a hand-crafted decision-round layer where only
//!   `(direct_commit_quorum - 1)` certifiers fully link to the voters. When the
//!   cap keeps the voters below `certificate_quorum`, no certificate forms and
//!   the anchor decides the target through the weak indirect rung instead —
//!   same assertion. With union certificates, `2f` certifiers (at most) keep
//!   the blocks that skip voters numerous enough for an anchor to miss them.
//!
//! For multi-leader cohorts, we sweep `target_offset` over `0..K`: the targeted
//! cohort leader is the one driven into indirect-commit, while the remaining
//! `K-1` cohort members direct-commit unchanged.

use std::sync::Arc;

use dag::{
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, build_split_chain, drop_leader},
};

use crate::{committer::Committer, leader::LeaderElector, protocol::ConsensusProtocol};

pub(crate) fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>) {
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    let k = protocol.leader_count.get();
    let elector = LeaderElector::new(committee.len(), protocol.leader_election);

    for target_offset in 0..k {
        let mut storage = Storage::new_for_test(committee);
        let mut committer = Committer::new_for_test(committee, &storage, spec);
        let l1 = committer.nth_leader_round(1);
        let target_leader = elector.elect_leader(l1, target_offset as u64);

        let l1_votes = build_dag(committee, &mut storage, None, l1);
        let l1_blames = drop_leader(&l1_votes, target_leader);
        let voting_round = committer.voting_round_for(l1);
        let decision_round = committer.decision_round_for(l1);

        let top_refs = if protocol.merged_certificates {
            let voters_count = (protocol.direct_commit_quorum - 1) as usize;
            let blamers_count = committee.len() - voters_count;
            let (supports, blames) = build_split_chain(
                committee,
                &mut storage,
                l1_votes,
                l1_blames,
                voting_round,
                blamers_count,
            );
            supports.into_iter().chain(blames).collect()
        } else {
            // Enough voters to back a certificate, capped one below the fast-path
            // commit quorum so fast-path protocols stay undecided at the voting
            // round. When the cap wins (certificate quorum >= fast commit quorum),
            // the voters intentionally fall below the certificate quorum: no
            // certificate forms and the anchor commits the target through the
            // weak indirect rung instead — same assertion.
            let voters_quorum = match &protocol.fast_path {
                Some(fast_path) => {
                    let below_fast_commit = fast_path.commit_quorum.saturating_sub(1);
                    protocol.certificate_quorum.min(below_fast_commit)
                }
                None => protocol.certificate_quorum,
            };
            let blamers_count = committee.len() - voters_quorum as usize;
            let (supports, blames) = build_split_chain(
                committee,
                &mut storage,
                l1_votes,
                l1_blames,
                voting_round,
                blamers_count,
            );
            let certifiers_count = if protocol.union_certificates {
                // Union certificates commit directly unless blocks of at least `n - 2f` stake can
                // all be missed by a later anchor, so at least that many must skip some voters.
                let faults = committee.total_stake() - protocol.quorum_threshold;
                ((2 * faults).min(protocol.direct_commit_quorum - 1)) as usize
            } else {
                (protocol.direct_commit_quorum - 1) as usize
            };
            let mut decision_refs = Vec::new();
            let certifier_connections = committee
                .authorities()
                .take(certifiers_count)
                .map(|authority| (authority, supports.clone()))
                .collect();
            decision_refs.extend(build_dag_layer(certifier_connections, &mut storage));

            let mixed_parents: Vec<_> = blames
                .into_iter()
                .chain(supports)
                .take(protocol.quorum_threshold as usize)
                .collect();
            let non_certifier_connections = committee
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
        tracing::info!("[{spec}] target_offset={target_offset} sequence: {sequence:?}");

        assert!(
            sequence.len() >= k,
            "[{spec}] target_offset={target_offset} expected at least {k} decisions"
        );
        for (offset, decision) in sequence.iter().take(k).enumerate() {
            let expected = elector.elect_leader(l1, offset as u64);
            if offset == target_offset {
                match decision {
                    LeaderStatus::IndirectCommit(block, _) => {
                        assert_eq!(
                            block.author(),
                            expected,
                            "[{spec}] target_offset={target_offset}"
                        );
                    }
                    other => panic!(
                        "[{spec}] target_offset={target_offset} expected IndirectCommit at \
                        offset {offset}, got {other:?}"
                    ),
                }
            } else {
                match decision {
                    LeaderStatus::DirectCommit(block, _) => {
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
