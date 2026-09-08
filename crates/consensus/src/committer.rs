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
    /// Reusable buffer for chain verdicts (adaptive Steelhead), ascending by
    /// round from `chain_floor`.
    chain: VecDeque<LeaderStatus>,
    /// Round of the first entry of `chain`.
    chain_floor: RoundNumber,
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
    /// updates fire. Entries start at interval boundaries, so the schedule is
    /// a deterministic function of the DAG alone, whatever the consumer's
    /// cadence.
    period_schedule: Vec<(RoundNumber, Option<NonZeroU64>)>,
    /// Number of completed interval scans (adaptive): the scan of interval `j`
    /// (rounds `j * interval + 1 ..= (j + 1) * interval`) fixes the period of
    /// interval `j + 1`.
    scans_done: u64,
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

    /// The wave of the chain verdict at `round`: the asynchronous wavelength
    /// at every round, whatever the round's own slot type.
    fn chain_wave(&self, round: RoundNumber) -> Wave {
        let wave_length = self.schedule.async_wave_length;
        Wave::new(wave_length, round % wave_length, self.merged_certificates)
    }

    /// The chain leader at `round`: the coin leader, so no known-leader slot
    /// lies on the chain.
    fn chain_leader(&self, round: RoundNumber) -> Authority {
        self.leader_elector.elect_fake_coin_leader(round)
    }

    /// The interval length; `None` when the period is static.
    fn interval(&self) -> Option<u64> {
        self.schedule.adaptive.map(|adaptive| adaptive.interval)
    }

    /// The highest round whose slot can be evaluated: the period is fixed up
    /// to the boundary of the interval under scan and unknown above it until
    /// that scan completes. Slots whose anchor search reaches above this
    /// round stay undecided until it moves.
    fn evaluation_top(&self, highest_known_round: RoundNumber) -> RoundNumber {
        match self.interval() {
            Some(interval) => highest_known_round.min((self.scans_done + 1) * interval),
            None => highest_known_round,
        }
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
            scans_done: 0,
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
            chain: VecDeque::new(),
            chain_floor: 0,
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

    /// Chain verdicts (adaptive Steelhead): the asynchronous rule read on every
    /// round from the interval under scan upward, with the round's coin leader
    /// and anchored on chain commits only. They never enter the output; the
    /// interval scan reads them to find the interval's anchor, which is why
    /// the period update keeps moving under asynchrony while known-leader
    /// slots are held undecided.
    fn compute_chain(&mut self) {
        self.chain.clear();
        let Some(mode) = &self.steelhead else {
            return;
        };
        let Some(interval) = mode.interval() else {
            return;
        };
        let highest_known_round = self.block_reader.highest_round();
        self.chain_floor = mode.scans_done * interval + 1;
        // Quorums are the pair's; wave and leader are per-call arguments.
        let committer = &self.base_committers[0];
        for round in (self.chain_floor..=highest_known_round).rev() {
            let wave = mode.chain_wave(round);
            let leader = mode.chain_leader(round);
            let mut status = committer.try_direct_decide(leader, round, wave);
            if !status.is_decided() {
                status = committer.try_indirect_decide(leader, round, self.chain.iter(), wave);
            }
            self.chain.push_front(status);
        }
    }

    /// Complete every interval scan the chain allows, in order. A scan walks
    /// the interval's rounds upward over chain verdicts, passing skips, until
    /// a chain commit (the interval's anchor) or the interval's end (no
    /// anchor: the period is kept); an undecided round leaves it incomplete.
    /// Each completed scan fixes the period of the next interval.
    fn complete_scans(&mut self) {
        loop {
            let Some(mode) = &self.steelhead else {
                return;
            };
            let Some(interval) = mode.interval() else {
                return;
            };
            let floor = mode.scans_done * interval + 1;
            let boundary = floor + interval - 1;
            let mut anchor = None;
            for round in floor..=boundary {
                let Some(status) = self.chain.get((round - self.chain_floor) as usize) else {
                    // The DAG does not reach this round yet.
                    return;
                };
                match status {
                    LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                        anchor = Some(block.clone());
                        break;
                    }
                    LeaderStatus::DirectSkip(..) | LeaderStatus::IndirectSkip(..) => continue,
                    LeaderStatus::Undecided(..) => return,
                }
            }
            self.apply_period_update(anchor, boundary);
        }
    }

    /// Fix the period of the interval above `boundary` from the window of the
    /// completed scan's anchor, if any, and record the scan as done.
    fn apply_period_update(&mut self, anchor: Option<Data<Block>>, boundary: RoundNumber) {
        let mode = self.steelhead.as_mut().expect("adaptive Steelhead");
        let adaptive = mode.schedule.adaptive.expect("adaptive Steelhead");
        let current = mode
            .period_at(boundary)
            .expect("adaptive periods are finite");

        let chosen = match anchor {
            Some(anchor) => {
                let window = replay::collect_window(&self.block_reader, &anchor, adaptive.interval);
                let mode = self.steelhead.as_ref().expect("checked above");
                let params = mode.replay_params.as_ref().expect("adaptive has params");
                replay::choose_period(
                    &window,
                    params,
                    current,
                    adaptive.max_period,
                    adaptive.epsilon_percent,
                )
            }
            None => current,
        };

        let mode = self.steelhead.as_mut().expect("checked above");
        if chosen != current {
            mode.period_schedule.push((boundary + 1, Some(chosen)));
            tracing::debug!("Steelhead period {current} -> {chosen} above round {boundary}");
        }
        mode.scans_done += 1;
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
        // Adaptive Steelhead: refresh the chain verdicts and complete the
        // interval scans they allow, fixing the periods of the intervals ahead.
        self.compute_chain();
        self.complete_scans();

        let highest_known_round = self.block_reader.highest_round();
        let top = match &self.steelhead {
            Some(mode) => mode.evaluation_top(highest_known_round),
            None => highest_known_round,
        };
        let last_decided_round = last_decided.map(|(round, _)| round).unwrap_or(0);

        // Try to decide as many leaders as possible, starting with the highest
        // evaluable round.
        self.leaders.clear();
        for round in (last_decided_round..=top).rev() {
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

        // The decided sequence is the longest prefix of decided leaders.
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
            // The wait always targets the public round-robin cohort — the
            // coin leader stays hidden. Async rounds wait only when canaried,
            // keeping the sync rule's evidence in the DAG at any period.
            if mode.is_async_round(round)
                && !mode
                    .schedule
                    .canary
                    .is_some_and(|canary| round.is_multiple_of(canary.get()))
            {
                return None;
            }
            let leaders = (0..mode.leader_count as RoundNumber)
                .map(move |leader_offset| mode.leader_elector.elect_leader(round + leader_offset));
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
