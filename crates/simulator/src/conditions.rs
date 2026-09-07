// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use dag::sync::network::NetworkMessage;
use rand::Rng;

use crate::{config::DelayModel, context::SimulatorContext};

/// Timed network-condition schedule: each phase applies a delay model (or
/// none, i.e. healthy) from its start time until the next phase begins.
/// Models add delay on top of the link latency; they never drop messages,
/// preserving eventual delivery.
pub struct NetworkConditions {
    /// Phases sorted by start time; the active one is the last started.
    phases: Vec<(Duration, Option<DelayModel>)>,
    committee_size: usize,
    /// Size of the attacked round-robin cohort per round.
    leader_count: usize,
    /// Highest block round seen on any link: the adversary's round tracker.
    max_round: AtomicU64,
}

impl NetworkConditions {
    pub fn new(
        phases: Vec<(Duration, Option<DelayModel>)>,
        committee_size: usize,
        leader_count: usize,
    ) -> Self {
        debug_assert!(phases.windows(2).all(|pair| pair[0].0 <= pair[1].0));
        Self {
            phases,
            committee_size,
            leader_count,
            max_round: AtomicU64::new(0),
        }
    }

    /// Extra delay the active model imposes on a message from `from` to `to`,
    /// on top of the link latency. Also feeds the round tracker. `to` is
    /// currently unused (the leader-delay model targets outbound traffic
    /// only) but kept for models that key on the destination.
    pub fn extra_delay(&self, from: usize, _to: usize, message: &NetworkMessage) -> Duration {
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
                // Mute, don't eclipse: only the cohort's outbound traffic is
                // delayed, so a targeted node still hears the network and
                // recovers the moment the schedule rotates past it.
                if self.sent_by_current_leader(from) {
                    Duration::from_millis(*delay_ms)
                } else {
                    Duration::ZERO
                }
            }
            Some(DelayModel::ScheduledAsynchrony { burst_ms }) => {
                let burst = Duration::from_millis(*burst_ms);
                let elapsed_in_burst =
                    Duration::from_nanos((now.as_nanos() % burst.as_nanos()) as u64);
                burst - elapsed_in_burst
            }
            Some(DelayModel::RandomLinkDelay {
                percent,
                delay_min_ms,
                delay_max_ms,
            }) => SimulatorContext::with_rng(|rng| {
                if rng.gen_range(0..100u8) < *percent {
                    Duration::from_millis(rng.gen_range(*delay_min_ms..=*delay_max_ms))
                } else {
                    Duration::ZERO
                }
            }),
            Some(DelayModel::ExponentialJitter {
                percent,
                base_ms,
                mean_ms,
                cap_ms,
            }) => SimulatorContext::with_rng(|rng| {
                if rng.gen_range(0..100u8) < *percent {
                    // Inverse-CDF sample of an exponential of mean `mean_ms`.
                    let uniform: f64 = rng.gen_range(0.0..1.0);
                    let jitter = (-(*mean_ms as f64) * (1.0 - uniform).ln()) as u64;
                    Duration::from_millis((*base_ms + jitter).min(*cap_ms))
                } else {
                    Duration::ZERO
                }
            }),
        }
    }

    /// Whether the sender is in the round-robin cohort of the tracked round.
    /// The adversary's strategy is protocol-independent: the public
    /// round-robin schedule is attacked whether or not the protocol under
    /// test relies on it, so every protocol faces the exact same adversary.
    /// Deliberately blind to the fake coin: hidden leaders stay hidden.
    fn sent_by_current_leader(&self, from: usize) -> bool {
        let round = self.max_round.load(Ordering::Relaxed);
        (0..self.leader_count as u64)
            .map(|offset| ((round + offset) % self.committee_size as u64) as usize)
            .any(|leader| leader == from)
    }
}
