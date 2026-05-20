// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{self, Display},
    sync::Arc,
};

use crate::{leader::LeaderElector, protocol::Protocol, wave::Wave};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Stake,
    committee::{Committee, StakeAggregator},
    consensus::LeaderStatus,
    data::Data,
    storage::BlockReader,
};

/// The [`BaseCommitter`] contains the bare bone commit
/// logic. Once instantiated, the methods
/// `try_direct_decide` and `try_indirect_decide` can be
/// called at any time and any number of times
/// (idempotent) to determine whether a leader can be
/// committed or skipped.
pub(crate) struct BaseCommitter {
    committee: Arc<Committee>,
    block_reader: BlockReader,
    leader_elector: LeaderElector,
    direct_commit_quorum: Stake,
    direct_skip_quorum: Stake,
    anchor_link_size: Stake,
    wave: Wave,
    leader_offset: RoundNumber,
}

impl BaseCommitter {
    pub(crate) fn new(
        committee: Arc<Committee>,
        block_reader: BlockReader,
        leader_elector: LeaderElector,
        protocol: &Protocol,
        leader_offset: RoundNumber,
        round_offset: RoundNumber,
    ) -> Self {
        Self {
            committee,
            block_reader,
            leader_elector,
            direct_commit_quorum: protocol.direct_commit_quorum,
            direct_skip_quorum: protocol.direct_skip_quorum,
            anchor_link_size: protocol.anchor_link_size,
            wave: Wave::new(protocol.wave_length, round_offset),
            leader_offset,
        }
    }

    #[cfg(test)]
    fn new_for_test(
        committee: &Arc<Committee>,
        block_reader: BlockReader,
        wave_length: RoundNumber,
        leader_offset: RoundNumber,
    ) -> Self {
        let total = committee.total_stake();
        let protocol = Protocol {
            direct_commit_quorum: 2 * total / 3 + 1,
            direct_skip_quorum: 2 * total / 3 + 1,
            anchor_link_size: 1,
            wave_length,
            leader_count: std::num::NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        };
        BaseCommitter::new(
            committee.clone(),
            block_reader,
            LeaderElector::new(committee.len()),
            &protocol,
            leader_offset,
            0,
        )
    }

    /// The leader-elect protocol is offset by `leader_offset` to ensure that different
    /// committers with different leader offsets elect different leaders for the same round number.
    /// This function returns `None` if there are no leaders for the specified round.
    pub(crate) fn elect_leader(&self, round: RoundNumber) -> Option<Authority> {
        if !self.wave.is_leader_round(round) {
            return None;
        }
        let offset = self.leader_offset as RoundNumber;
        Some(self.leader_elector.elect_leader(round + offset))
    }

    /// Find which block is supported at (author, round) by the given block.
    /// Blocks can indirectly reference multiple other blocks at (author, round), but only one
    /// block at (author, round) will be supported by the given block.
    fn find_support(
        &self,
        (author, round): (Authority, RoundNumber),
        from: &Data<Block>,
    ) -> Option<BlockReference> {
        if from.round() < round {
            return None;
        }
        for include in from.includes() {
            // Weak links may point to blocks with lower round numbers than strong links.
            if include.round() < round {
                continue;
            }
            if include.author_round() == (author, round) {
                return Some(*include);
            }
            let include = self
                .block_reader
                .get_block(*include)
                .expect("We should have the whole sub-dag by now");
            if let Some(support) = self.find_support((author, round), &include) {
                return Some(support);
            }
        }
        None
    }

    /// Check whether the specified block (`potential_certificate`) is a vote for
    /// the specified leader (`leader_block`).
    fn is_vote(&self, potential_vote: &Data<Block>, leader_block: &Data<Block>) -> bool {
        let (author, round) = leader_block.author_round();
        self.find_support((author, round), potential_vote) == Some(*leader_block.reference())
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`).
    fn is_certificate(
        &self,
        potential_certificate: &Data<Block>,
        leader_block: &Data<Block>,
    ) -> bool {
        // When the certificate sits exactly one round above the leader (i.e.
        // `wave_length == 2`), votes and certificates coincide on the same round:
        // each decision-round block that directly supports the leader is itself a
        // certificate.
        if potential_certificate.round() == leader_block.round() + 1 {
            return self.is_vote(potential_certificate, leader_block);
        }

        let mut votes_stake_aggregator = StakeAggregator::new(self.direct_commit_quorum);
        for reference in potential_certificate.includes() {
            let potential_vote = self
                .block_reader
                .get_block(*reference)
                .expect("We should have the whole sub-dag by now");

            if self.is_vote(&potential_vote, leader_block) {
                tracing::trace!("[{self}] {potential_vote:?} is a vote for {leader_block:?}");
                if votes_stake_aggregator.add(reference.authority, &self.committee) {
                    tracing::trace!(
                        "[{self}] {potential_certificate:?} is a certificate for {leader_block:?}"
                    );
                    return true;
                }
            }
        }
        false
    }

    /// Decide the status of a target leader from the specified anchor. We commit the target leader
    /// if it has a certified link to the anchor. Otherwise, we skip the target leader.
    fn decide_leader_from_anchor(
        &self,
        anchor: &Data<Block>,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        // Get the block(s) proposed by the leader. There could be more than one leader block
        // per round (produced by a Byzantine leader).
        let leader_blocks = self
            .block_reader
            .get_blocks_at_authority_round(leader, leader_round);

        // Get all blocks that could be potential certificates for the target leader. These blocks
        // are in the decision round of the target leader and are linked to the anchor.
        let wave = self.wave.number(leader_round);
        let decision_round = self.wave.decision_round(wave);
        let decision_blocks = self.block_reader.get_blocks_by_round(decision_round);

        // Find the certified leader block (at most one).
        let mut certified = leader_blocks.into_iter().filter(|leader_block| {
            let mut aggregator = StakeAggregator::new(self.anchor_link_size);
            decision_blocks.iter().any(|block| {
                if !self.block_reader.linked(anchor, block) {
                    return false;
                }
                if !self.is_certificate(block, leader_block) {
                    return false;
                }
                aggregator.add(block.author(), &self.committee)
            })
        });
        let first = certified.next();
        if certified.next().is_some() {
            panic!("More than one certified block at wave {wave} from leader {leader}")
        }

        tracing::trace!("[{self}] leader {leader} is decided by anchor {anchor:?}");
        match first {
            Some(block) => LeaderStatus::IndirectCommit(block.clone()),
            None => LeaderStatus::IndirectSkip(leader, leader_round),
        }
    }

    /// Check whether the specified leader has enough blames (`direct_skip_quorum` non-votes)
    /// to be directly skipped.
    fn enough_leader_blame(
        &self,
        voting_round: RoundNumber,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> bool {
        let voting_blocks = self.block_reader.get_blocks_by_round(voting_round);

        let mut aggregator = StakeAggregator::new(self.direct_skip_quorum);
        for voting_block in &voting_blocks {
            let voter = voting_block.reference().authority;
            if self
                .find_support((leader, leader_round), voting_block)
                .is_none()
            {
                tracing::trace!(
                    "[{self}] {voting_block:?} is a blame for leader {}",
                    leader.with_round(leader_round)
                );
                if aggregator.add(voter, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether the specified leader has enough support (`direct_commit_quorum`
    /// certificates) to be directly committed.
    fn enough_leader_support(
        &self,
        decision_round: RoundNumber,
        leader_block: &Data<Block>,
    ) -> bool {
        let decision_blocks = self.block_reader.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::new(self.direct_commit_quorum);
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(decision_block, leader_block)
                && certificate_stake_aggregator.add(authority, &self.committee)
            {
                return true;
            }
        }
        false
    }

    /// Apply the indirect decision rule to the specified leader to see whether we can
    /// indirect-commit or indirect-skip it.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(leader = %leader.with_round(leader_round))
    )]
    pub(crate) fn try_indirect_decide<'a>(
        &self,
        leader: Authority,
        leader_round: RoundNumber,
        leaders: impl Iterator<Item = &'a LeaderStatus>,
    ) -> LeaderStatus {
        // The anchor is the first committed leader with round higher than the decision round of the
        // target leader. We must stop the iteration upon encountering an undecided leader.
        let anchors = leaders.filter(|x| leader_round + self.wave.length() <= x.round());

        for anchor in anchors {
            tracing::trace!(
                "[{self}] Trying to indirect-decide {} using anchor {anchor}",
                leader.with_round(leader_round),
            );
            match anchor {
                LeaderStatus::DirectCommit(anchor) | LeaderStatus::IndirectCommit(anchor) => {
                    return self.decide_leader_from_anchor(anchor, leader, leader_round);
                }
                LeaderStatus::DirectSkip(..) | LeaderStatus::IndirectSkip(..) => (),
                LeaderStatus::Undecided(..) => break,
            }
        }

        LeaderStatus::Undecided(leader, leader_round)
    }

    /// Apply the direct decision rule to the specified leader to see whether we can direct-commit
    /// or direct-skip it.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(leader = %leader.with_round(leader_round))
    )]
    pub(crate) fn try_direct_decide(
        &self,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        let wave = self.wave.number(leader_round);
        let voting_round = self.wave.voting_round(wave);
        let decision_round = self.wave.decision_round(wave);

        // Check whether the leader has enough blame. That is, whether there are
        // `direct_skip_quorum` non-votes for that leader (which ensure there will never
        // be a certificate for that leader).
        if self.enough_leader_blame(voting_round, leader, leader_round) {
            return LeaderStatus::DirectSkip(leader, leader_round);
        }

        // Check whether the leader(s) has enough support. That is, whether there are
        // `direct_commit_quorum` certificates over the leader. Note that there could be
        // more than one leader block (created by Byzantine leaders).
        let leader_blocks = self
            .block_reader
            .get_blocks_at_authority_round(leader, leader_round);
        let mut supported = leader_blocks
            .into_iter()
            .filter(|l| self.enough_leader_support(decision_round, l));
        let first = supported.next();
        if supported.next().is_some() {
            panic!(
                "[{self}] More than one certified block for {}",
                leader.with_round(leader_round)
            )
        }

        first
            .map(LeaderStatus::DirectCommit)
            .unwrap_or_else(|| LeaderStatus::Undecided(leader, leader_round))
    }
}

impl Display for BaseCommitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Committer-L{}-R{}",
            self.leader_offset,
            self.wave.round_offset(),
        )
    }
}

#[cfg(test)]
mod tests {

    use dag::{
        authority::Authority,
        consensus::LeaderStatus,
        storage::Storage,
        test_util::{build_dag, build_dag_layer, committee},
    };

    use crate::base::BaseCommitter;

    /// `elect_leader` returns `None` outside leader rounds.
    #[test]
    fn elect_leader_none_at_non_leader_round() {
        let committee = committee(4);
        let (storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);
        // wave_length = 3, round_offset = 0 → leader rounds {0, 3, 6, ...}.
        assert!(committer.elect_leader(0).is_some());
        assert!(committer.elect_leader(1).is_none());
        assert!(committer.elect_leader(2).is_none());
        assert!(committer.elect_leader(3).is_some());
        assert!(committer.elect_leader(4).is_none());
        assert!(committer.elect_leader(6).is_some());
    }

    /// `leader_offset` shifts the elected authority within the round-robin schedule.
    #[test]
    fn elect_leader_offset_shifts_authority() {
        let committee = committee(4);
        let (storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        let c0 = BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);
        let c1 = BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 1);
        assert_ne!(c0.elect_leader(3), c1.elect_leader(3));
    }

    /// `find_support` returns the direct include when one points at the target slot.
    #[test]
    fn find_support_direct_include() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 2);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let block_at_2 = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(0u64), 2)
            .into_iter()
            .next()
            .unwrap();

        let r = committer
            .find_support((Authority::from(1u64), 1), &block_at_2)
            .expect("fully connected DAG must include round-1 (auth=1)");
        assert_eq!(r.authority, Authority::from(1u64));
        assert_eq!(r.round, 1);
    }

    /// `find_support` walks transitively through includes.
    #[test]
    fn find_support_transitive_walk() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 3);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let block_at_3 = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(0u64), 3)
            .into_iter()
            .next()
            .unwrap();

        // The round-3 block's direct includes are at round 2; finding (1, 1) requires
        // walking through one of those round-2 blocks to locate the round-1 reference.
        let r = committer
            .find_support((Authority::from(1u64), 1), &block_at_3)
            .expect("transitive walk must locate the round-1 (auth=1) block");
        assert_eq!(r.authority, Authority::from(1u64));
        assert_eq!(r.round, 1);
    }

    /// `find_support` returns `None` when the source block is below the target round.
    #[test]
    fn find_support_below_target_round_returns_none() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 1);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let block_at_1 = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(0u64), 1)
            .into_iter()
            .next()
            .unwrap();

        assert!(
            committer
                .find_support((Authority::from(1u64), 2), &block_at_1)
                .is_none(),
        );
    }

    /// At `wave_length = 2`, votes and certificates coincide on the same round:
    /// `is_certificate` reduces to `is_vote`.
    #[test]
    fn is_certificate_wave_length_two_reduces_to_vote() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 3);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 2, 0);

        let leader_block = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(0u64), 2)
            .into_iter()
            .next()
            .unwrap();
        let cert_candidate = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(1u64), 3)
            .into_iter()
            .next()
            .unwrap();

        assert!(committer.is_certificate(&cert_candidate, &leader_block));
    }

    /// At `wave_length = 3`, a fully-connected DAG produces a strong-quorum of votes
    /// under each decision-round block, so every decision-round block is a certificate.
    #[test]
    fn is_certificate_wave_length_three_full_dag() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 5);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader_block = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(0u64), 3)
            .into_iter()
            .next()
            .unwrap();
        let cert_candidate = storage
            .block_reader()
            .get_blocks_at_authority_round(Authority::from(1u64), 5)
            .into_iter()
            .next()
            .unwrap();

        assert!(committer.is_certificate(&cert_candidate, &leader_block));
    }

    /// `enough_leader_blame` is `true` when every non-leader voter omits the leader.
    #[test]
    fn enough_leader_blame_when_all_non_leader_voters_blame() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);

        let leader_round = 3;
        let refs_at_leader = build_dag(&committee, &mut storage, None, leader_round);
        let leader = Authority::from(0u64);
        let refs_without_leader: Vec<_> = refs_at_leader
            .into_iter()
            .filter(|r| r.authority != leader)
            .collect();
        let connections = committee
            .authorities()
            .filter(|&a| a != leader)
            .map(|a| (a, refs_without_leader.clone()))
            .collect();
        build_dag_layer(connections, &mut storage);

        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);
        assert!(committer.enough_leader_blame(4, leader, leader_round));
    }

    /// `enough_leader_support` is `true` for a fully-connected DAG.
    #[test]
    fn enough_leader_support_when_full_dag() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 5);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader = Authority::from(0u64);
        let leader_block = storage
            .block_reader()
            .get_blocks_at_authority_round(leader, 3)
            .into_iter()
            .next()
            .unwrap();
        assert!(committer.enough_leader_support(5, &leader_block));
    }

    /// `try_direct_decide` issues `DirectCommit` on a fully-connected DAG.
    #[test]
    fn try_direct_decide_commits_on_full_dag() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 5);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader = committer.elect_leader(3).unwrap();
        match committer.try_direct_decide(leader, 3) {
            LeaderStatus::DirectCommit(b) => assert_eq!(b.author(), leader),
            other => panic!("expected DirectCommit, got {other:?}"),
        }
    }

    /// `try_direct_decide` issues `DirectSkip` when the leader is unanimously blamed.
    #[test]
    fn try_direct_decide_skips_when_leader_omitted() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);

        let refs = build_dag(&committee, &mut storage, None, 3);
        // Round-robin: round 3 → authority 3.
        let leader = committee.authorities().nth(3).unwrap();
        let refs_without_leader: Vec<_> =
            refs.into_iter().filter(|r| r.authority != leader).collect();
        let voting_connections = committee
            .authorities()
            .filter(|&a| a != leader)
            .map(|a| (a, refs_without_leader.clone()))
            .collect();
        let refs_at_4 = build_dag_layer(voting_connections, &mut storage);
        build_dag(&committee, &mut storage, Some(refs_at_4), 5);

        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);
        match committer.try_direct_decide(leader, 3) {
            LeaderStatus::DirectSkip(skipped, round) => {
                assert_eq!(skipped, leader);
                assert_eq!(round, 3);
            }
            other => panic!("expected DirectSkip, got {other:?}"),
        }
    }

    /// `try_direct_decide` reports `Undecided` when neither support nor blame reaches quorum.
    #[test]
    fn try_direct_decide_undecided_on_insufficient_dag() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        // DAG only reaches the leader round → nothing has voted yet.
        build_dag(&committee, &mut storage, None, 3);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader = committer.elect_leader(3).unwrap();
        match committer.try_direct_decide(leader, 3) {
            LeaderStatus::Undecided(a, r) => {
                assert_eq!(a, leader);
                assert_eq!(r, 3);
            }
            other => panic!("expected Undecided, got {other:?}"),
        }
    }

    /// `try_indirect_decide` returns `Undecided` when the anchor iterator is empty.
    #[test]
    fn try_indirect_decide_undecided_on_empty_anchors() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 3);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader = committer.elect_leader(3).unwrap();
        match committer.try_indirect_decide(leader, 3, std::iter::empty()) {
            LeaderStatus::Undecided(a, r) => {
                assert_eq!(a, leader);
                assert_eq!(r, 3);
            }
            other => panic!("expected Undecided, got {other:?}"),
        }
    }

    /// `try_indirect_decide` short-circuits when an undecided anchor appears in the iterator.
    #[test]
    fn try_indirect_decide_breaks_on_undecided_anchor() {
        let committee = committee(4);
        let (mut storage, _) = Storage::new_for_test(Authority::from(0u64), &committee);
        build_dag(&committee, &mut storage, None, 3);
        let committer =
            BaseCommitter::new_for_test(&committee, storage.block_reader().clone(), 3, 0);

        let leader = committer.elect_leader(3).unwrap();
        let undecided_anchor = LeaderStatus::Undecided(Authority::from(0u64), 6);
        let anchors = [undecided_anchor];
        match committer.try_indirect_decide(leader, 3, anchors.iter()) {
            LeaderStatus::Undecided(a, r) => {
                assert_eq!(a, leader);
                assert_eq!(r, 3);
            }
            other => panic!("expected Undecided after undecided anchor, got {other:?}"),
        }
    }
}
