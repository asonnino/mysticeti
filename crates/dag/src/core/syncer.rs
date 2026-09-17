// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Notify;

use super::{Core, block_handler::CommitHandler};
use crate::consensus::{CommittedSubDag, DagConsensus};
use crate::storage::Storage;
use crate::{
    authority::Authority,
    block::{Block, RoundNumber},
    context::Ctx,
    data::Data,
    metrics::Metrics,
};

pub struct Syncer<C: Ctx, D: DagConsensus> {
    core: Core<C, D>,
    force_new_block: bool,
    signals: SyncerSignals,
    commit_handler: CommitHandler<C>,
    pub(crate) connected_authorities: HashSet<Authority>,
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

impl<C: Ctx, D: DagConsensus> Syncer<C, D> {
    pub fn new(
        core: Core<C, D>,
        signals: SyncerSignals,
        commit_handler: CommitHandler<C>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let committee_size = core.committee().len();
        Self {
            core,
            force_new_block: false,
            signals,
            commit_handler,
            connected_authorities: HashSet::with_capacity(committee_size),
            metrics,
        }
    }

    /// Add blocks to the core, returning any sub-dags newly committed as a consequence.
    ///
    /// The commit rule runs with every own proposal. For protocols whose fast commit
    /// quorum exceeds the threshold-clock quorum it also runs when the added blocks
    /// complete that quorum at a watched voting round (see [`Core::fast_commit_watch`]):
    /// the fast commit would otherwise wait for the next proposal, a round later.
    pub fn add_blocks(&mut self, blocks: Vec<Data<Block>>) -> Vec<CommittedSubDag> {
        let _timer = self.metrics.utilization_timer("Syncer::add_blocks");
        let watch = self.core.fast_commit_watch();
        self.core.add_blocks(blocks);
        if self.try_propose()
            || watch.is_some_and(|watch| self.core.fast_commit_quorum_crossed(&watch))
        {
            self.commit()
        } else {
            Vec::new()
        }
    }

    /// Force a new block proposal, returning any sub-dags newly committed as a consequence.
    pub fn force_new_block(&mut self, round: RoundNumber) -> Vec<CommittedSubDag> {
        if self.core.last_proposed() == round {
            self.metrics.inc_leader_timeout();
            self.force_new_block = true;
            self.try_new_block()
        } else {
            Vec::new()
        }
    }

    fn try_new_block(&mut self) -> Vec<CommittedSubDag> {
        if self.try_propose() {
            self.commit()
        } else {
            Vec::new()
        }
    }

    /// Propose a new block if the core is ready for one; true when a block was proposed.
    fn try_propose(&mut self) -> bool {
        let _timer = self.metrics.utilization_timer("Syncer::try_new_block");
        let ready = self.force_new_block || self.core.ready_new_block(&self.connected_authorities);
        if !ready || self.core.try_new_block().is_none() {
            return false;
        }
        self.signals.new_block_ready();
        self.force_new_block = false;
        true
    }

    /// Run the commit rule and hand the newly committed sub-dags to the commit handler.
    fn commit(&mut self) -> Vec<CommittedSubDag> {
        let _timer = self.metrics.utilization_timer("Syncer::commit");
        let newly_committed = self.core.try_commit();
        let utc_now = C::timestamp_utc();
        if !newly_committed.is_empty() {
            let committed_refs: Vec<_> = newly_committed
                .iter()
                .map(|block| {
                    let age = utc_now.checked_sub(block.timestamp()).unwrap_or_default();
                    format!("{}({}ms)", block.reference(), age.as_millis())
                })
                .collect();
            tracing::debug!("Committed {:?}", committed_refs);
        }
        let committed_subdag = self
            .commit_handler
            .handle_commit(self.core.block_reader(), newly_committed);
        self.core.handle_committed_subdag(committed_subdag)
    }

    pub fn commit_handler(&self) -> &CommitHandler<C> {
        &self.commit_handler
    }

    pub fn core(&self) -> &Core<C, D> {
        &self.core
    }

    /// Consume the syncer and yield the owned [`Storage`] — see [`Core::into_storage`].
    pub fn into_storage(self) -> Storage {
        self.core.into_storage()
    }

    pub fn connect_authority(&mut self, authority: Authority) {
        self.connected_authorities.insert(authority);
    }

    pub fn disconnect_authority(&mut self, authority: Authority) {
        self.connected_authorities.remove(&authority);
    }
}
