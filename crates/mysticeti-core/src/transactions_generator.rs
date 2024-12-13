// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::min, collections::VecDeque, sync::Arc, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::sync::mpsc;

use crate::{
    committee::Committee,
    config::{ClientParameters, LoadType, NodePublicConfig},
    crypto::AsBytes,
    metrics::Metrics,
    runtime::{self, timestamp_utc},
    types::{AuthorityIndex, Transaction},
};

pub struct TransactionGenerator {
    committee: Arc<Committee>,
    sender: mpsc::Sender<Vec<Transaction>>,
    rng: StdRng,
    client_parameters: ClientParameters,
    node_public_config: NodePublicConfig,
    metrics: Arc<Metrics>,
    budget: u64,
}

impl TransactionGenerator {
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);

    pub fn start(
        committee: Arc<Committee>,
        sender: mpsc::Sender<Vec<Transaction>>,
        seed: AuthorityIndex,
        client_parameters: ClientParameters,
        node_public_config: NodePublicConfig,
        metrics: Arc<Metrics>,
    ) {
        assert!(client_parameters.transaction_size > 8 + 8); // 8 bytes timestamp + 8 bytes random
        tracing::info!(
            "Starting generator with {} transactions per second, initial delay {:?}",
            client_parameters.load,
            client_parameters.initial_delay
        );

        let budget = Self::initial_budget(&client_parameters.load_type, &committee);
        runtime::Handle::current().spawn(
            Self {
                sender,
                rng: StdRng::seed_from_u64(seed),
                client_parameters,
                node_public_config,
                metrics,
                budget,
                committee,
            }
            .run(),
        );
    }

    pub async fn run(mut self) {
        let load = self.client_parameters.load;
        let transactions_per_block_interval = if load <= 1000 { load } else { (load + 9) / 10 };
        let target_block_interval = if load <= 1000 {
            Duration::from_secs(1)
        } else {
            Self::TARGET_BLOCK_INTERVAL
        };

        tracing::warn!(
            "Generating {transactions_per_block_interval} transactions per {} ms",
            target_block_interval.as_millis()
        );
        if let LoadType::BCounter { total_budget } = self.client_parameters.load_type {
            tracing::warn!("Total budget: {}", total_budget);
        }

        let max_block_size = self.node_public_config.parameters.max_block_size;
        let target_block_size = min(max_block_size, transactions_per_block_interval);

        let mut counter = 0;
        let mut tx_to_report = 0;
        let mut random: u64 = self.rng.gen(); // 8 bytes
        let zeros = vec![0u8; self.client_parameters.transaction_size - 8 - 8]; // 8 bytes timestamp + 8 bytes random

        let mut buffer = VecDeque::new();
        let metrics_granularity = if load > 10_000 { 10_000 } else { 1 };

        let mut interval = runtime::TimeInterval::new(target_block_interval);
        runtime::sleep(self.client_parameters.initial_delay).await;
        tracing::warn!("Starting to submit transactions");
        loop {
            interval.tick().await;
            let timestamp = (timestamp_utc().as_millis() as u64).to_le_bytes();

            let mut block = Vec::with_capacity(target_block_size);
            let mut block_size = 0;
            for _ in 0..transactions_per_block_interval {
                random += counter;

                let mut transaction = Vec::with_capacity(self.client_parameters.transaction_size);
                transaction.extend_from_slice(&timestamp); // 8 bytes
                transaction.extend_from_slice(&random.to_le_bytes()); // 8 bytes
                transaction.extend_from_slice(&zeros[..]);

                buffer.push_front(Transaction::new(transaction));
                self.update_budget(counter);

                for _ in 0..self.budget {
                    tracing::debug!("Submitted tx: counter={counter}, budget={}", self.budget);
                    let Some(tx) = buffer.pop_back() else { break };
                    block.push(tx);

                    block_size += self.client_parameters.transaction_size;
                    counter += 1;
                    tx_to_report += 1;

                    if block_size >= max_block_size {
                        if self.sender.send(block.clone()).await.is_err() {
                            return;
                        }
                        block.clear();
                        block_size = 0;
                    }
                }
            }

            if !block.is_empty() && self.sender.send(block).await.is_err() {
                return;
            }

            if counter % metrics_granularity == 0 {
                self.metrics.submitted_transactions.inc_by(tx_to_report);
                tracing::debug!("Submitted {tx_to_report} transactions");
                tx_to_report = 0;
                self.metrics.budget.set(self.budget as i64);
                self.metrics.tx_buffer.set(buffer.len() as i64);
            }
        }
    }

    pub fn extract_timestamp(transaction: &Transaction) -> Duration {
        let bytes = transaction.as_bytes()[0..8]
            .try_into()
            .expect("Transactions should be at least 8 bytes");
        Duration::from_millis(u64::from_le_bytes(bytes))
    }

    fn certified_transactions_count(&self) -> u64 {
        self.metrics
            .latency_s
            .get_metric_with_label_values(&["owned"])
            .map_or_else(|_| 0, |m| m.get_sample_count())
    }

    fn initial_budget(load_type: &LoadType, committee: &Arc<Committee>) -> u64 {
        match load_type {
            LoadType::Sui { sequential } => {
                if *sequential {
                    1
                } else {
                    18000000000000000 // very large number, but not quite u64::MAX to avoid overflow
                }
            }
            LoadType::BCounter { total_budget } => {
                (total_budget * committee.validity_threshold()) / committee.quorum_threshold()
            }
        }
    }

    fn update_budget(&mut self, counter: u64) {
        let committee_size = self.committee.len() as u64;
        let certified_transactions_count = self.certified_transactions_count();
        let unique_certificates = if certified_transactions_count % committee_size == 0 {
            certified_transactions_count / committee_size
        } else {
            certified_transactions_count / committee_size + 1
        };
        tracing::debug!(
            "updating budget: certified_transactions_count={certified_transactions_count}, unique_certificates={unique_certificates}, counter={counter}"
        );

        match self.client_parameters.load_type {
            LoadType::Sui { .. } => {
                self.budget += unique_certificates;
                self.budget = self.budget.saturating_sub(counter);
            }
            LoadType::BCounter { total_budget } => {
                self.budget = self.budget.saturating_sub(counter);
                // Budget is exhausted.
                if self.budget == 0 {
                    // Client is ready to send a version update (piggy-backed on its next transaction).
                    if unique_certificates == counter {
                        // only if N clients
                        let remaining_budget = total_budget - counter;
                        tracing::debug!("Merge: remaining_budget={remaining_budget}");
                        self.budget = (remaining_budget * self.committee.validity_threshold())
                            / self.committee.quorum_threshold()
                    }
                }
            }
        }
        tracing::debug!("updated budget: {}", self.budget);
    }
}
