// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::{
    block_handler::CommitHandler,
    core::Core,
    data::Data,
    metrics::Metrics,
    runtime::timestamp_utc,
    types::{AuthorityIndex, RoundNumber, StatementBlock},
};

pub struct Syncer {
    core: Core,
    force_new_block: bool,
    commit_period: u64,
    signals: SyncerSignals,
    commit_handler: CommitHandler,
    pub(crate) connected_authorities: HashSet<AuthorityIndex>,
    metrics: Arc<Metrics>,
}

pub struct SyncerSignals {
    notify: Option<Arc<Notify>>,
    new_block: bool,
}

impl SyncerSignals {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify: Some(notify),
            new_block: false,
        }
    }

    pub fn test() -> Self {
        Self {
            notify: None,
            new_block: false,
        }
    }

    pub fn new_block_ready(&mut self) {
        self.new_block = true;
        if let Some(notify) = &self.notify {
            notify.notify_waiters();
        }
    }

    pub fn take_new_block(&mut self) -> bool {
        std::mem::take(&mut self.new_block)
    }
}

impl Syncer {
    pub fn new(
        core: Core,
        commit_period: u64,
        signals: SyncerSignals,
        commit_handler: CommitHandler,
        metrics: Arc<Metrics>,
    ) -> Self {
        let committee_size = core.committee().len();
        Self {
            core,
            force_new_block: false,
            commit_period,
            signals,
            commit_handler,
            connected_authorities: HashSet::with_capacity(committee_size),
            metrics,
        }
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) {
        let _timer = self.metrics.utilization_timer("Syncer::add_blocks");
        self.core.add_blocks(blocks);
        self.try_new_block();
    }

    pub fn force_new_block(&mut self, round: RoundNumber) -> bool {
        if self.core.last_proposed() == round {
            self.metrics.inc_leader_timeout();
            self.force_new_block = true;
            self.try_new_block();
            true
        } else {
            false
        }
    }

    fn try_new_block(&mut self) {
        let _timer = self.metrics.utilization_timer("Syncer::try_new_block");
        if self.force_new_block
            || self
                .core
                .ready_new_block(self.commit_period, &self.connected_authorities)
        {
            if self.core.try_new_block().is_none() {
                return;
            }
            self.signals.new_block_ready();
            self.force_new_block = false;

            if self.core.epoch_closed() {
                return;
            }; // No need to commit after epoch is safe to close

            let newly_committed = self.core.try_commit();
            let utc_now = timestamp_utc();
            if !newly_committed.is_empty() {
                let committed_refs: Vec<_> = newly_committed
                    .iter()
                    .map(|block| {
                        let age = utc_now
                            .checked_sub(block.meta_creation_time())
                            .unwrap_or_default();
                        format!("{}({}ms)", block.reference(), age.as_millis())
                    })
                    .collect();
                tracing::debug!("Committed {:?}", committed_refs);
            }
            let committed_subdag = self
                .commit_handler
                .handle_commit(self.core.block_reader(), newly_committed);
            self.core.handle_committed_subdag(committed_subdag);
        }
    }

    pub fn commit_handler(&self) -> &CommitHandler {
        &self.commit_handler
    }

    pub fn core(&self) -> &Core {
        &self.core
    }

    #[cfg(test)]
    pub fn scheduler_state_id(&self) -> usize {
        self.core.authority() as usize
    }
}

#[cfg(test)]
#[cfg(feature = "simulator")]
mod tests {
    use std::{ops::Range, time::Duration};

    use rand::Rng;

    use super::*;
    use crate::{
        data::Data,
        simulator::{Scheduler, Simulator, SimulatorState},
        test_util::{check_commits, committee_and_syncers, rng_at_seed},
    };

    const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1800);

    pub enum SyncerEvent {
        ForceNewBlock(RoundNumber),
        DeliverBlock(Data<StatementBlock>),
    }

    impl SimulatorState for Syncer {
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

            if self.signals.take_new_block() {
                let last_block = self.core.last_own_block().clone();
                Scheduler::schedule_event(
                    ROUND_TIMEOUT,
                    self.scheduler_state_id(),
                    SyncerEvent::ForceNewBlock(last_block.round()),
                );
                for authority in self.core.committee().authorities() {
                    if authority == self.core.authority() {
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
                .all(|s| s.core.last_proposed() >= target_round)
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
}
