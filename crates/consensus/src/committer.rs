// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::VecDeque,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    base::BaseCommitter,
    leader::LeaderElector,
    protocol::{Protocol, SteelheadSchedule},
    replay::{self, ReplayParams},
    wave::Wave,
};
use dag::{
    authority::Authority,
    block::{Block, RoundNumber},
    committee::Committee,
    committee::Stake,
    consensus::{DagConsensus, LeaderStatus},
    data::Data,
    metrics::Metrics,
    storage::BlockReader,
};

#[cfg(any(test, feature = "test-utils"))]
use crate::protocol::ConsensusProtocol;
#[cfg(any(test, feature = "test-utils"))]
use dag::storage::Storage;

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct Committer {
    block_reader: BlockReader,
    base_committers: Vec<BaseCommitter>,
    quorum_threshold: Stake,
    leader_wait: bool,
    /// Whether the protocol has an optimistic fast path; drives the test-only
    /// round-depth queries.
    #[cfg(any(test, feature = "test-utils"))]
    has_fast_path: bool,
    /// Reusable buffer for commit decisions.
    leaders: VecDeque<LeaderStatus>,
    /// Steelhead's per-round wavelength mode; `None` for the base protocols.
    steelhead: Option<SteelheadMode>,
    /// Gauge target for period updates (adaptive Steelhead only).
    metrics: Option<Arc<Metrics>>,
    /// Live-period cell shared with the round-timeout task and, eventually,
    /// the simulated adversary (0 encodes an infinite period).
    period_cell: Option<Arc<AtomicU64>>,
}

/// Steelhead state: the wavelength schedule plus the leader source, which must
/// not go through `BaseCommitter::elect_leader` (its stored wave would reject
/// off-cycle rounds; under Steelhead every round hosts a leader slot).
struct SteelheadMode {
    schedule: SteelheadSchedule,
    merged_certificates: bool,
    leader_elector: LeaderElector,
    leader_count: usize,
    /// The period in force from each round on; a single entry unless adaptive
    /// updates fire. Deterministic function of the consumed commit sequence.
    period_schedule: Vec<(RoundNumber, Option<NonZeroU64>)>,
    /// Round of the last interval anchor (adaptive).
    last_update_round: RoundNumber,
    /// An interval anchor yielded but not yet replayed; the update applies at
    /// the start of the next `try_commit`. Relies on the consumer fully
    /// consuming the yielded prefix (as `Core::try_commit` does).
    pending_anchor: Option<Data<Block>>,
    /// Replay inputs, present iff adaptive.
    replay_params: Option<ReplayParams>,
}

impl SteelheadMode {
    /// The period in force at `round`: the last schedule entry at or below it.
    fn period_at(&self, round: RoundNumber) -> Option<NonZeroU64> {
        self.period_schedule
            .iter()
            .rev()
            .find(|(start, _)| *start <= round)
            .map(|(_, period)| *period)
            .expect("the period schedule covers round zero")
    }

    fn is_async_round(&self, round: RoundNumber) -> bool {
        matches!(self.period_at(round), Some(period) if round.is_multiple_of(period.get()))
    }

    /// The wave governing the slot at `round`: its own wavelength, aligned so
    /// that `round` is a genuine leader round.
    fn wave_for(&self, round: RoundNumber) -> Wave {
        let wave_length = if self.is_async_round(round) {
            self.schedule.async_wave_length
        } else {
            self.schedule.sync_wave_length
        };
        Wave::new(wave_length, round % wave_length, self.merged_certificates)
    }

    fn elect_leader(&self, round: RoundNumber, leader_offset: RoundNumber) -> Authority {
        if self.is_async_round(round) {
            return self
                .leader_elector
                .elect_fake_coin_leader(round + leader_offset);
        }
        self.leader_elector.elect_leader(round + leader_offset)
    }

    /// The round past which the next yielded commit becomes the interval
    /// anchor; `None` when the period is static.
    fn update_threshold(&self) -> Option<RoundNumber> {
        self.schedule
            .adaptive
            .map(|adaptive| self.last_update_round + adaptive.interval)
    }
}

impl Committer {
    pub fn new(committee: Arc<Committee>, block_reader: BlockReader, protocol: Protocol) -> Self {
        let mut base_committers = Vec::new();
        // Steelhead assigns waves per round, so a single stage of base
        // committers (one per leader offset) evaluates every slot.
        let pipeline_stages = if protocol.steelhead.is_some() {
            1
        } else if protocol.pipeline {
            protocol.wave_length
        } else {
            1
        };

        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..protocol.leader_count.get() {
                let committer = BaseCommitter::new(
                    committee.clone(),
                    block_reader.clone(),
                    LeaderElector::new(committee.len()),
                    &protocol,
                    leader_offset as RoundNumber,
                    round_offset,
                );
                base_committers.push(committer);
            }
        }

        let steelhead = protocol.steelhead.map(|schedule| SteelheadMode {
            schedule,
            merged_certificates: protocol.merged_certificates,
            leader_elector: LeaderElector::new(committee.len()),
            leader_count: protocol.leader_count.get(),
            period_schedule: vec![(0, schedule.period)],
            last_update_round: 0,
            pending_anchor: None,
            replay_params: schedule
                .adaptive
                .and_then(|_| ReplayParams::from_protocol(&protocol, committee.clone())),
        });

        Self {
            block_reader,
            base_committers,
            quorum_threshold: protocol.quorum_threshold,
            leader_wait: protocol.leader_wait,
            #[cfg(any(test, feature = "test-utils"))]
            has_fast_path: protocol.fast_path.is_some(),
            leaders: VecDeque::new(),
            steelhead,
            metrics: None,
            period_cell: None,
        }
    }

    /// Attach the metrics sink for the period gauge (adaptive Steelhead).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach the live-period cell shared with the round-timeout task.
    pub fn with_period_cell(mut self, period_cell: Arc<AtomicU64>) -> Self {
        self.period_cell = Some(period_cell);
        self
    }

    /// Apply a pending adaptive-period update: replay the interval anchor's
    /// window and adopt the chosen period from the round after the anchor.
    fn apply_pending_period_update(&mut self) {
        let Some(mode) = self.steelhead.as_mut() else {
            return;
        };
        let Some(anchor) = mode.pending_anchor.take() else {
            return;
        };
        let Some(adaptive) = mode.schedule.adaptive else {
            return;
        };
        let anchor_round = anchor.round();
        let current = mode
            .period_at(anchor_round)
            .expect("adaptive periods are finite");

        let window = replay::collect_window(&self.block_reader, &anchor, adaptive.interval);
        let mode = self.steelhead.as_mut().expect("checked above");
        let params = mode.replay_params.as_ref().expect("adaptive has params");
        let chosen = replay::choose_period(
            &window,
            params,
            current,
            adaptive.max_period,
            adaptive.epsilon_percent,
        );

        if chosen != current {
            mode.period_schedule.push((anchor_round + 1, Some(chosen)));
            tracing::debug!("Steelhead period {current} -> {chosen} after anchor {anchor_round}");
        }
        mode.last_update_round = anchor_round;
        if let Some(metrics) = &self.metrics {
            metrics.set_steelhead_period(chosen.get());
        }
        if let Some(cell) = &self.period_cell {
            cell.store(chosen.get(), Ordering::Relaxed);
        }
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders. `last_decided` is the slot of the most recently consumed
    /// decision; pass `None` on a fresh start to yield every decided leader from round 0
    /// upward.
    #[tracing::instrument(level = "debug", skip_all, fields(last_decided = ?last_decided))]
    pub fn try_commit(
        &mut self,
        last_decided: Option<(RoundNumber, Authority)>,
    ) -> impl Iterator<Item = LeaderStatus> + '_ {
        // A truncated previous pass left an interval anchor: adopt its period
        // before re-evaluating the rounds above it.
        self.apply_pending_period_update();

        let highest_known_round = self.block_reader.highest_round();
        let last_decided_round = last_decided.map(|(round, _)| round).unwrap_or(0);

        // Try to decide as many leaders as possible, starting with the highest round.
        self.leaders.clear();
        for round in (last_decided_round..=highest_known_round).rev() {
            if let Some(mode) = &self.steelhead {
                // Steelhead: every round hosts a leader slot, decided under the
                // wave its schedule assigns to that round.
                let wave = mode.wave_for(round);
                for (leader_offset, committer) in self.base_committers.iter().enumerate().rev() {
                    let leader = mode.elect_leader(round, leader_offset as RoundNumber);
                    let mut status = committer.try_direct_decide(leader, round, wave);
                    if !status.is_decided() {
                        status =
                            committer.try_indirect_decide(leader, round, self.leaders.iter(), wave);
                    }
                    self.leaders.push_front(status);
                }
            } else {
                for committer in self.base_committers.iter().rev() {
                    // Skip committers that don't have a leader for this round.
                    let Some(leader) = committer.elect_leader(round) else {
                        continue;
                    };
                    tracing::debug!(
                        "Trying to decide {} with {committer}",
                        leader.with_round(round)
                    );

                    // Try to directly decide the leader.
                    let mut status = committer.try_direct_decide(leader, round, committer.wave);
                    tracing::debug!("Outcome of direct rule: {status}");

                    // If we can't directly decide the leader, try to indirectly decide it.
                    if !status.is_decided() {
                        status = committer.try_indirect_decide(
                            leader,
                            round,
                            self.leaders.iter(),
                            committer.wave,
                        );
                        tracing::debug!("Outcome of indirect rule: {status}");
                    }

                    self.leaders.push_front(status);
                }
            }
        }

        // The decided sequence is the longest prefix of decided leaders,
        // truncated at an interval anchor when the adaptive period is on: the
        // rounds above the anchor are re-evaluated next call under the period
        // the anchor's replay chooses.
        let steelhead = &mut self.steelhead;
        self.leaders
            .drain(..)
            // Position past the previously-yielded decision, if any. When `None`
            // (fresh start), yield every decided leader from round 0 upward.
            .skip_while(move |x| match last_decided {
                Some(round_author) => (x.round(), x.authority()) != round_author,
                None => false,
            })
            .skip(if last_decided.is_some() { 1 } else { 0 })
            // Filter out all the genesis.
            .filter(|x| x.round() > 0)
            // Stop the sequence upon encountering an undecided leader.
            .take_while(|x| x.is_decided())
            .scan(false, move |truncated, status| {
                if *truncated {
                    return None;
                }
                if let Some(mode) = steelhead.as_mut()
                    && let Some(threshold) = mode.update_threshold()
                    && status.round() > threshold
                {
                    let block = match &status {
                        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                            Some(block.clone())
                        }
                        _ => None,
                    };
                    if let Some(block) = block {
                        mode.pending_anchor = Some(block);
                        *truncated = true;
                    }
                }
                Some(status)
            })
            .inspect(|x| tracing::debug!("Decided {x}"))
    }
}

/// Test-only constructor and round-arithmetic queries.
#[cfg(any(test, feature = "test-utils"))]
impl Committer {
    /// Build a [`Committer`] over the given storage from a [`ConsensusProtocol`]
    /// spec. Mirrors [`Storage::new_for_test`] so integration tests can stamp
    /// out fresh committers without depending on `BlockReader` or
    /// [`Protocol::to_protocol`] internals.
    pub fn new_for_test(
        committee: &Arc<Committee>,
        storage: &Storage,
        spec: &ConsensusProtocol,
    ) -> Self {
        Self::new(
            committee.clone(),
            storage.block_reader().clone(),
            spec.to_protocol(committee).expect("valid protocol"),
        )
    }

    /// The leader this committer elects at `(round, leader_offset)`, following
    /// the protocol's leader source (round-robin or the fake coin).
    /// Panics if `round` is not a leader round or the offset has no committer.
    pub fn leader_at(&self, round: RoundNumber, leader_offset: RoundNumber) -> Authority {
        if let Some(mode) = &self.steelhead {
            return mode.elect_leader(round, leader_offset);
        }
        self.base_committers
            .iter()
            .filter(|bc| bc.leader_offset() == leader_offset)
            .find_map(|bc| bc.elect_leader(round))
            .expect("not a leader round for this offset")
    }

    /// True if any of this committer's base committers owns a leader at `round`.
    /// Under Steelhead, every round hosts a leader slot.
    pub fn is_leader_round(&self, round: RoundNumber) -> bool {
        if self.steelhead.is_some() {
            return true;
        }
        self.base_committers
            .iter()
            .any(|bc| bc.wave.is_leader_round(round))
    }

    /// Smallest leader round strictly greater than `round`.
    pub fn next_leader_round_after(&self, round: RoundNumber) -> RoundNumber {
        (round + 1..)
            .find(|&r| self.is_leader_round(r))
            .expect("leader rounds are unbounded above")
    }

    /// The `n`-th leader round counting from 0 (1-indexed: `n=1` returns the
    /// first non-genesis leader round).
    pub fn nth_leader_round(&self, n: u64) -> RoundNumber {
        let mut round = 0;
        for _ in 0..n {
            round = self.next_leader_round_after(round);
        }
        round
    }

    /// The wave governing the slot at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    fn wave_for(&self, leader_round: RoundNumber) -> Wave {
        if let Some(mode) = &self.steelhead {
            return mode.wave_for(leader_round);
        }
        self.base_committers
            .iter()
            .find(|bc| bc.wave.is_leader_round(leader_round))
            .expect("not a leader round")
            .wave
    }

    /// Wavelength of the wave governing the slot at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    pub fn wave_length_at(&self, leader_round: RoundNumber) -> RoundNumber {
        self.wave_for(leader_round).length()
    }

    /// Voting round for the leader at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    pub fn voting_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        let wave = self.wave_for(leader_round);
        wave.voting_round(wave.number(leader_round))
    }

    /// Decision round for the leader at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    pub fn decision_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        let wave = self.wave_for(leader_round);
        wave.decision_round(wave.number(leader_round))
    }

    /// Shallowest DAG depth at which the direct rule can decide the leader at
    /// `leader_round`: the voting round when a fast path is configured (votes
    /// alone can commit), else the decision round.
    /// Panics if `leader_round` is not a leader round.
    pub fn earliest_decision_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        if self.has_fast_path {
            self.voting_round_for(leader_round)
        } else {
            self.decision_round_for(leader_round)
        }
    }
}

impl DagConsensus for Committer {
    fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold
    }

    fn try_commit(
        &mut self,
        last_decided: Option<(RoundNumber, Authority)>,
    ) -> impl Iterator<Item = LeaderStatus> {
        self.try_commit(last_decided)
    }

    fn get_leaders(&self, round: RoundNumber) -> Option<impl Iterator<Item = Authority>> {
        if let Some(mode) = &self.steelhead {
            // No leader wait on async slots: their leader is meant to be hidden.
            if mode.is_async_round(round) {
                return None;
            }
            // Compute the sync-slot leaders directly: the base committers'
            // election would yield an empty iterator on off-cycle rounds,
            // vacuously satisfying the wait.
            let leaders = (0..mode.leader_count as RoundNumber)
                .map(move |leader_offset| mode.elect_leader(round, leader_offset));
            return Some(LeaderIter::Steelhead(leaders));
        }
        if self.leader_wait {
            Some(LeaderIter::Base(
                self.base_committers
                    .iter()
                    .filter_map(move |c| c.elect_leader(round)),
            ))
        } else {
            None
        }
    }
}

/// Unifies the two `get_leaders` iterator types behind one return type.
enum LeaderIter<A, B> {
    Base(A),
    Steelhead(B),
}

impl<A, B> Iterator for LeaderIter<A, B>
where
    A: Iterator<Item = Authority>,
    B: Iterator<Item = Authority>,
{
    type Item = Authority;

    fn next(&mut self) -> Option<Authority> {
        match self {
            Self::Base(leaders) => leaders.next(),
            Self::Steelhead(leaders) => leaders.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::{consensus::DagConsensus, storage::Storage, test_util::committee};

    use crate::{
        committer::Committer,
        protocol::{FastPath, Protocol},
    };

    fn test_protocol(fast_path: Option<FastPath>) -> Protocol {
        Protocol {
            direct_commit_quorum: 3,
            direct_skip_quorum: 3,
            certificate_quorum: 3,
            quorum_threshold: 4,
            fast_path,
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
            steelhead: None,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: true,
            require_crypto: false,
        }
    }

    #[test]
    fn quorum_threshold_sourced_from_protocol_field() {
        let committee = committee(4);
        let storage = Storage::new_for_test(&committee);
        let committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(None),
        );
        assert_eq!(committer.quorum_threshold(), 4);
    }

    /// `earliest_decision_round_for` is the decision round for single-tier protocols
    /// and the voting round when a fast path is configured.
    #[test]
    fn earliest_decision_round_tracks_fast_path() {
        let committee = committee(4);
        let storage = Storage::new_for_test(&committee);
        let slow_committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(None),
        );
        let fast_committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(Some(FastPath {
                commit_quorum: 3,
                weak_indirect_quorum: 2,
            })),
        );
        // Wave length 3: leader round 3 → voting round 4, decision round 5.
        assert_eq!(slow_committer.earliest_decision_round_for(3), 5);
        assert_eq!(fast_committer.earliest_decision_round_for(3), 4);
    }
}
