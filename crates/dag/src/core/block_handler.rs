// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::{
    consensus::{CommittedSubDag, Linearizer},
    context::Ctx,
    data::Data,
    metrics::Metrics,
    storage::BlockReader,
    types::{Block, BlockReference, Transaction, TransactionLocator},
};

pub struct RealBlockHandler<C: Ctx> {
    pub transaction_time: Arc<Mutex<HashMap<TransactionLocator, C::Instant>>>,
    metrics: Arc<Metrics>,
    receiver: mpsc::Receiver<Vec<Transaction>>,
    pending_transactions: usize,
}

/// The max number of transactions per block.
// todo - This value should be in bytes because it is capped by the wal entry size.
pub const SOFT_MAX_PROPOSED_PER_BLOCK: usize = 20 * 1000;

impl<C: Ctx> RealBlockHandler<C> {
    pub fn new(metrics: Arc<Metrics>) -> (Self, mpsc::Sender<Vec<Transaction>>) {
        let (sender, receiver) = mpsc::channel(1024);
        let this = Self {
            transaction_time: Default::default(),
            metrics,
            receiver,
            pending_transactions: 0, // todo - need to initialize correctly when loaded from disk
        };
        (this, sender)
    }

    fn receive_with_limit(&mut self) -> Option<Vec<Transaction>> {
        if self.pending_transactions >= SOFT_MAX_PROPOSED_PER_BLOCK {
            return None;
        }
        let received = self.receiver.try_recv().ok()?;
        self.pending_transactions += received.len();
        Some(received)
    }

    pub fn handle_blocks(&mut self, require_response: bool) -> Vec<Transaction> {
        let _timer = self
            .metrics
            .utilization_timer("BlockHandler::handle_blocks");
        let mut response = vec![];
        if require_response {
            while let Some(data) = self.receive_with_limit() {
                response.extend(data);
            }
        }
        response
    }

    pub fn handle_proposal(&mut self, block: &Data<Block>) {
        let mut transaction_time = self.transaction_time.lock();
        let mut count = 0usize;
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, C::now());
            count += 1;
        }
        self.pending_transactions -= count;
    }

    pub fn cleanup(&self) {
        let _timer = self.metrics.block_handler_cleanup_utilization_timer();
        let mut l = self.transaction_time.lock();
        l.retain(|_k, v| C::elapsed(v) < Duration::from_secs(10));
    }
}

pub struct CommitHandler<C: Ctx> {
    commit_interpreter: Linearizer,
    committed_leaders: Vec<BlockReference>,
    start_time: C::Instant,
    transaction_time: Arc<Mutex<HashMap<TransactionLocator, C::Instant>>>,
    metrics: Arc<Metrics>,
}

impl<C: Ctx> CommitHandler<C> {
    pub fn new(
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, C::Instant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            commit_interpreter: Linearizer::new(),
            committed_leaders: vec![],
            start_time: C::now(),
            transaction_time,
            metrics,
        }
    }

    pub fn committed_leaders(&self) -> &[BlockReference] {
        &self.committed_leaders
    }

    fn update_metrics(
        &self,
        block_creation: Option<&C::Instant>,
        current_timestamp: Duration,
        transaction: &Transaction,
    ) {
        if let Some(instant) = block_creation {
            let latency = C::elapsed(instant);
            self.metrics.observe_transaction_committed_latency(latency);
            self.metrics
                .observe_inter_block_latency_s("shared", latency.as_secs_f64());
        }

        let time_from_start = C::elapsed(&self.start_time);
        let benchmark_duration = self.metrics.benchmark_duration_secs();
        if let Some(delta) = time_from_start.as_secs().checked_sub(benchmark_duration) {
            self.metrics.inc_benchmark_duration_by(delta);
        }

        let tx_submission_timestamp = transaction.extract_timestamp();
        let latency = current_timestamp.saturating_sub(tx_submission_timestamp);
        let square_latency = latency.as_secs_f64().powf(2.0);
        self.metrics
            .observe_latency_s("shared", latency.as_secs_f64());
        self.metrics
            .observe_latency_squared_s("shared", square_latency);
    }

    pub fn handle_commit(
        &mut self,
        block_reader: &BlockReader,
        committed_leaders: Vec<Data<Block>>,
    ) -> Vec<CommittedSubDag> {
        let current_timestamp = C::timestamp_utc();

        let committed = self
            .commit_interpreter
            .handle_commit(block_reader, committed_leaders);
        let transaction_time = self.transaction_time.lock();
        for commit in &committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                for (locator, transaction) in block.shared_transactions() {
                    self.update_metrics(
                        transaction_time.get(&locator),
                        current_timestamp,
                        transaction,
                    );
                }
            }
        }
        committed
    }

    pub fn recover_committed(&mut self, committed: HashSet<BlockReference>) {
        assert!(self.commit_interpreter.committed.is_empty());
        self.commit_interpreter.committed = committed;
    }
}
