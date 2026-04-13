// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, SystemTime},
};

use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio::{sync::mpsc, time};

use dag::{
    block::types::Transaction,
    config::{ClientParameters, NodePublicConfig},
    metrics::Metrics,
    types::AuthorityIndex,
};

pub struct TransactionGenerator {
    sender: mpsc::Sender<Vec<Transaction>>,
    rng: StdRng,
    client_parameters: ClientParameters,
    node_public_config: NodePublicConfig,
    metrics: Arc<Metrics>,
}

impl TransactionGenerator {
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);

    pub fn start(
        sender: mpsc::Sender<Vec<Transaction>>,
        seed: AuthorityIndex,
        client_parameters: ClientParameters,
        node_public_config: NodePublicConfig,
        metrics: Arc<Metrics>,
    ) {
        assert!(client_parameters.transaction_size > 8 + 8);
        tracing::info!(
            "Starting generator with {} transactions per second, initial delay {:?}",
            client_parameters.load,
            client_parameters.initial_delay
        );
        tokio::spawn(
            Self {
                sender,
                rng: StdRng::seed_from_u64(seed),
                client_parameters,
                node_public_config,
                metrics,
            }
            .run(),
        );
    }

    async fn run(mut self) {
        let load = self.client_parameters.load;
        let transactions_per_block_interval = load.div_ceil(10);
        tracing::info!(
            "Generating {transactions_per_block_interval} transactions per {} ms",
            Self::TARGET_BLOCK_INTERVAL.as_millis()
        );
        let max_block_size = self.node_public_config.parameters.max_block_size;
        let target_block_size = min(max_block_size, transactions_per_block_interval);

        let mut counter = 0;
        let mut tx_to_report = 0;
        let mut random: u64 = self.rng.r#gen();
        let zeros = vec![0u8; self.client_parameters.transaction_size - 16];

        let mut interval = time::interval(Self::TARGET_BLOCK_INTERVAL);
        time::sleep(self.client_parameters.initial_delay).await;
        loop {
            interval.tick().await;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            let timestamp = (now.as_millis() as u64).to_le_bytes();

            let mut block = Vec::with_capacity(target_block_size);
            let mut block_size = 0;
            for _ in 0..transactions_per_block_interval {
                random += counter;

                let mut transaction = Vec::with_capacity(self.client_parameters.transaction_size);
                transaction.extend_from_slice(&timestamp);
                transaction.extend_from_slice(&random.to_le_bytes());
                transaction.extend_from_slice(&zeros[..]);

                block.push(Transaction::new(transaction));
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

            if !block.is_empty() && self.sender.send(block).await.is_err() {
                return;
            }

            if counter % 10_000 == 0 {
                self.metrics.inc_submitted_transactions(tx_to_report);
                tx_to_report = 0
            }
        }
    }
}
