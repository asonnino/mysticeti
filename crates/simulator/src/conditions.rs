// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use dag::sync::{net_sync::QuorumTimeoutRounds, network::NetworkMessage};

use crate::{config::DelayModel, context::SimulatorContext};

/// Timed network-condition schedule: each phase applies a delay model (or
/// none, i.e. healthy) from its start time until the next phase begins.
/// Models add delay on top of the link latency; they never drop messages,
/// preserving eventual delivery.
pub struct NetworkConditions {
    /// Phases sorted by start time; the active one is the last started.
    phases: Vec<(Duration, Option<DelayModel>)>,
    committee_size: usize,
    /// Leaders per round of the protocol under test (the attacked cohort).
    leader_count: usize,
    /// Which rounds have NO known leader (the quorum-cap rounds): the
    /// adversary cannot target those — the coin is hidden even though the
    /// period is public. `Every` means the protocol is never targetable.
    quorum_rounds: QuorumTimeoutRounds,
    /// Highest block round seen on any link: the adversary's round tracker.
    max_round: AtomicU64,
}

impl NetworkConditions {
    pub fn new(
        phases: Vec<(Duration, Option<DelayModel>)>,
        committee_size: usize,
        leader_count: usize,
        quorum_rounds: QuorumTimeoutRounds,
    ) -> Self {
        debug_assert!(phases.windows(2).all(|pair| pair[0].0 <= pair[1].0));
        Self {
            phases,
            committee_size,
            leader_count,
            quorum_rounds,
            max_round: AtomicU64::new(0),
        }
    }

    /// Extra delay the active model imposes on a message from `from` to `to`,
    /// on top of the link latency. Also feeds the round tracker.
    pub fn extra_delay(&self, from: usize, to: usize, message: &NetworkMessage) -> Duration {
        if let NetworkMessage::Block(block) = message {
            self.max_round.fetch_max(block.round(), Ordering::Relaxed);
        }

        let now = SimulatorContext::time();
        let model = self
            .phases
            .iter()
            .take_while(|(start, _)| *start <= now)
            .last()
            .and_then(|(_, model)| model.as_ref());
        match model {
            None => Duration::ZERO,
            Some(DelayModel::TargetedLeaderDelay { delay_ms }) => {
                if self.touches_current_leaders(from, to) {
                    Duration::from_millis(*delay_ms)
                } else {
                    Duration::ZERO
                }
            }
        }
    }

    /// Whether either endpoint is a current known leader: the round-robin
    /// cohort of the tracked round, on rounds that have a known leader at
    /// all. Deliberately blind to the fake coin, so async slots (and fully
    /// asynchronous protocols) remain untargetable.
    fn touches_current_leaders(&self, from: usize, to: usize) -> bool {
        let round = self.max_round.load(Ordering::Relaxed);
        let known_leader_round = match self.quorum_rounds {
            QuorumTimeoutRounds::None => true,
            QuorumTimeoutRounds::Every => false,
            QuorumTimeoutRounds::EveryNth(period) => !round.is_multiple_of(period.get()),
        };
        if !known_leader_round {
            return false;
        }
        (0..self.leader_count as u64)
            .map(|offset| ((round + offset) % self.committee_size as u64) as usize)
            .any(|leader| leader == from || leader == to)
    }
}
