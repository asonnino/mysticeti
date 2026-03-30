// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use minibytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::{
    consensus::linearizer::{CommittedSubDag, Linearizer},
    data::Data,
    metrics::Metrics,
    runtime::{self, TimeInstant},
    storage::BlockReader,
    transactions_generator::TransactionGenerator,
    types::{BaseStatement, BlockReference, StatementBlock, Transaction, TransactionLocator},
};

pub struct RealBlockHandler {
    pub transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
    metrics: Arc<Metrics>,
    receiver: mpsc::Receiver<Vec<Transaction>>,
    pending_transactions: usize,
}

/// The max number of transactions per block.
// todo - This value should be in bytes because it is capped by the wal entry size.
pub const SOFT_MAX_PROPOSED_PER_BLOCK: usize = 20 * 1000;

impl RealBlockHandler {
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

    pub fn handle_blocks(
        &mut self,
        _blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement> {
        let _timer = self
            .metrics
            .utilization_timer("BlockHandler::handle_blocks");
        let mut response = vec![];
        if require_response {
            while let Some(data) = self.receive_with_limit() {
                for tx in data {
                    response.push(BaseStatement::Share(tx));
                }
            }
        }
        response
    }

    pub fn handle_proposal(&mut self, block: &Data<StatementBlock>) {
        let mut transaction_time = self.transaction_time.lock();
        let mut count = 0usize;
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, TimeInstant::now());
            count += 1;
        }
        self.pending_transactions -= count;
    }

    pub fn state(&self) -> Bytes {
        Bytes::new()
    }

    pub fn recover_state(&mut self, _state: &Bytes) {}

    pub fn cleanup(&self) {
        let _timer = self.metrics.block_handler_cleanup_utilization_timer();
        let mut l = self.transaction_time.lock();
        l.retain(|_k, v| v.elapsed() < Duration::from_secs(10));
    }
}

pub struct CommitHandler {
    commit_interpreter: Linearizer,
    committed_leaders: Vec<BlockReference>,
    start_time: TimeInstant,
    transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
    metrics: Arc<Metrics>,
}

impl CommitHandler {
    pub fn new(
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            commit_interpreter: Linearizer::new(),
            committed_leaders: vec![],
            start_time: TimeInstant::now(),
            transaction_time,
            metrics,
        }
    }

    pub fn committed_leaders(&self) -> &[BlockReference] {
        &self.committed_leaders
    }

    /// Note: these metrics are used to compute performance during benchmarks.
    fn update_metrics(
        &self,
        block_creation: Option<&TimeInstant>,
        current_timestamp: Duration,
        transaction: &Transaction,
    ) {
        // Record inter-block latency.
        if let Some(instant) = block_creation {
            let latency = instant.elapsed();
            self.metrics.observe_transaction_committed_latency(latency);
            self.metrics
                .observe_inter_block_latency_s("shared", latency.as_secs_f64());
        }

        // Record benchmark start time.
        let time_from_start = self.start_time.elapsed();
        let benchmark_duration = self.metrics.benchmark_duration_secs();
        if let Some(delta) = time_from_start.as_secs().checked_sub(benchmark_duration) {
            self.metrics.inc_benchmark_duration_by(delta);
        }

        // Record end-to-end latency. The first 8 bytes of the transaction are the timestamp of the
        // transaction submission.
        let tx_submission_timestamp = TransactionGenerator::extract_timestamp(transaction);
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
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let current_timestamp = runtime::timestamp_utc();

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

    pub fn aggregator_state(&self) -> Bytes {
        Bytes::new()
    }

    pub fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>) {
        assert!(self.commit_interpreter.committed.is_empty());
        if state.is_some() {
            assert!(!committed.is_empty());
        } else {
            assert!(committed.is_empty());
        }
        self.commit_interpreter.committed = committed;
    }
}
