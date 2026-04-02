// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::Range, time::Duration};

use rand::Rng;

use super::super::event_simulator::{Scheduler, Simulator, SimulatorState};
use super::super::test_util::committee_and_syncers;
use crate::context::Ctx;
use crate::data::Data;
use crate::syncer::Syncer;
use crate::test_util::{check_commits, rng_at_seed};
use crate::types::{RoundNumber, StatementBlock};

const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1800);

pub enum SyncerEvent {
    ForceNewBlock(RoundNumber),
    DeliverBlock(Data<StatementBlock>),
}

impl<C: Ctx> SimulatorState for Syncer<C> {
    type Event = SyncerEvent;

    fn handle_event(&mut self, event: Self::Event) {
        match event {
            SyncerEvent::ForceNewBlock(round) => {
                self.force_new_block(round);
            }
            SyncerEvent::DeliverBlock(block) => {
                self.add_blocks(vec![block]);
            }
        }

        if self.take_new_block() {
            let last_block = self.core().last_own_block().clone();
            Scheduler::schedule_event(
                ROUND_TIMEOUT,
                self.scheduler_state_id(),
                SyncerEvent::ForceNewBlock(last_block.round()),
            );
            for authority in self.core().committee().authorities() {
                if authority == self.core().authority() {
                    continue;
                }
                let latency =
                    Scheduler::<SyncerEvent>::with_rng(|rng| rng.gen_range(LATENCY_RANGE));
                Scheduler::schedule_event(
                    latency,
                    authority as usize,
                    SyncerEvent::DeliverBlock(last_block.clone()),
                );
            }
        }
    }
}

#[test]
pub fn test_syncer() {
    for seed in 0..10 {
        test_syncer_at(seed);
    }
}

pub fn test_syncer_at(seed: u64) {
    eprintln!("Seed {seed}");
    let rng = rng_at_seed(seed);
    let (committee, syncers) = committee_and_syncers(4);
    let mut simulator = Simulator::new(syncers, rng);

    for authority in committee.authorities() {
        simulator.schedule_event(
            Duration::ZERO,
            authority as usize,
            SyncerEvent::ForceNewBlock(0),
        );
    }

    let target_round = 40;
    loop {
        assert!(!simulator.run_one());
        if simulator
            .states()
            .iter()
            .all(|s| s.core().last_proposed() >= target_round)
        {
            let time = simulator.time();
            eprintln!(
                "All syncers reached round {target_round} \
                in {time:.2?}",
            );
            check_commits(simulator.states());
            break;
        }
    }
}
