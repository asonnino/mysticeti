// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Notify;

use super::{Core, block_handler::CommitHandler};
use crate::{
    context::Ctx,
    data::Data,
    metrics::Metrics,
    types::{AuthorityIndex, RoundNumber, StatementBlock},
};

pub struct Syncer<C: Ctx> {
    core: Core<C>,
    force_new_block: bool,
    commit_period: u64,
    signals: SyncerSignals,
    commit_handler: CommitHandler<C>,
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

impl<C: Ctx> Syncer<C> {
    pub fn new(
        core: Core<C>,
        commit_period: u64,
        signals: SyncerSignals,
        commit_handler: CommitHandler<C>,
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

            let newly_committed = self.core.try_commit();
            let utc_now = C::timestamp_utc();
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

    pub fn commit_handler(&self) -> &CommitHandler<C> {
        &self.commit_handler
    }

    pub fn core(&self) -> &Core<C> {
        &self.core
    }
}
