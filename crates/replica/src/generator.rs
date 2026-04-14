// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    mem,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use minibytes::Bytes;
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
        let tx_per_interval = load.div_ceil(10);
        let tx_size = self.client_parameters.transaction_size;
        tracing::info!(
            "Generating {tx_per_interval} transactions per {} ms",
            Self::TARGET_BLOCK_INTERVAL.as_millis()
        );
        let max_block_size = self.node_public_config.parameters.max_block_size;
        let block_capacity = (max_block_size / tx_size).min(tx_per_interval);

        let mut counter = 0u64;
        let mut tx_to_report = 0u64;
        let mut random: u64 = self.rng.r#gen();
        let batch_size = tx_size * tx_per_interval;
        let mut buffer = vec![0u8; batch_size];

        let base_system_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let base_instant = Instant::now();

        let mut interval = time::interval(Self::TARGET_BLOCK_INTERVAL);
        time::sleep(self.client_parameters.initial_delay).await;
        loop {
            interval.tick().await;
            let elapsed = base_instant.elapsed();
            let timestamp_ms = (base_system_time + elapsed).as_millis() as u64;

            // Fill the reusable buffer with all transactions.
            for i in 0..tx_per_interval {
                random += counter;
                let offset = i * tx_size;
                buffer[offset..offset + 8].copy_from_slice(&timestamp_ms.to_le_bytes());
                buffer[offset + 8..offset + 16].copy_from_slice(&random.to_le_bytes());
                counter += 1;
            }
            let batch = Bytes::from(mem::replace(&mut buffer, vec![0u8; batch_size]));

            let mut block = Vec::with_capacity(block_capacity);
            let mut block_size = 0;
            for i in 0..tx_per_interval {
                let tx_bytes = batch.slice(i * tx_size..(i + 1) * tx_size);
                block.push(Transaction::new(tx_bytes));
                block_size += tx_size;

                if block_size >= max_block_size {
                    let full_block = mem::replace(&mut block, Vec::with_capacity(block_capacity));
                    if self.sender.send(full_block).await.is_err() {
                        return;
                    }
                    block_size = 0;
                }
            }

            if !block.is_empty() && self.sender.send(block).await.is_err() {
                return;
            }

            tx_to_report += tx_per_interval as u64;
            if counter.is_multiple_of(10_000) {
                self.metrics.inc_submitted_transactions(tx_to_report);
                tx_to_report = 0
            }
        }
    }
}
