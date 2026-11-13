// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{mem, sync::Arc, time::Duration};

use minibytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use dag::{authority::Authority, block::transaction::Transaction, context::Ctx, metrics::Metrics};

use crate::{client::TransactionClient, config::LoadGeneratorConfig};

pub struct TransactionGenerator {
    client: TransactionClient,
    max_block_size: usize,
    metrics: Arc<Metrics>,
}

impl TransactionGenerator {
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);
    const HEADER_SIZE: usize = 8 + 8; // timestamp + random

    pub fn start<C: Ctx>(
        client: TransactionClient,
        seed: Authority,
        config: LoadGeneratorConfig,
        max_block_size: usize,
        metrics: Arc<Metrics>,
    ) -> C::JoinHandle<()> {
        assert!(
            config.transaction_size > Self::HEADER_SIZE,
            "transaction_size must be greater than {} bytes",
            Self::HEADER_SIZE
        );
        tracing::info!(
            "Starting generator with {} transactions per second, initial delay {:?}",
            config.load,
            config.initial_delay
        );
        let mut rng = StdRng::seed_from_u64(seed.as_u64());
        let random = rng.r#gen();
        C::spawn(
            Self {
                client,
                max_block_size,
                metrics,
            }
            .run::<C>(config, random),
        )
    }

    fn fill_batch(
        buffer: &mut Vec<u8>,
        transaction_size: usize,
        transactions_per_interval: usize,
        timestamp_ms: u64,
        counter: &mut u64,
        random: &mut u64,
    ) -> Bytes {
        for i in 0..transactions_per_interval {
            *random += *counter;
            let offset = i * transaction_size;
            buffer[offset..offset + 8].copy_from_slice(&timestamp_ms.to_le_bytes());
            buffer[offset + 8..offset + 16].copy_from_slice(&random.to_le_bytes());
            *counter += 1;
        }
        let batch_size = transaction_size * transactions_per_interval;
        Bytes::from(mem::replace(buffer, vec![0u8; batch_size]))
    }

    async fn ship_blocks(
        &self,
        batch: &Bytes,
        transaction_size: usize,
        transactions_per_interval: usize,
        block_capacity: usize,
    ) -> bool {
        let mut block = Vec::with_capacity(block_capacity);
        let mut block_size = 0;
        for i in 0..transactions_per_interval {
            let start = i * transaction_size;
            let end = start + transaction_size;
            block.push(Transaction::new(batch.slice(start..end)));
            block_size += transaction_size;

            if block_size >= self.max_block_size {
                let full_block = mem::replace(&mut block, Vec::with_capacity(block_capacity));
                if self.client.submit(full_block).await.is_err() {
                    return false;
                }
                block_size = 0;
            }
        }
        if !block.is_empty() {
            return self.client.submit(block).await.is_ok();
        }
        true
    }

    async fn run<C: Ctx>(self, config: LoadGeneratorConfig, mut random: u64) {
        let load = config.load;
        let transaction_size = config.transaction_size;
        let intervals_per_second = 1000 / Self::TARGET_BLOCK_INTERVAL.as_millis() as usize;
        let transactions_per_interval = load.div_ceil(intervals_per_second);
        tracing::info!(
            "Generating up to {transactions_per_interval} transactions per {} ms",
            Self::TARGET_BLOCK_INTERVAL.as_millis()
        );
        let block_capacity =
            (self.max_block_size / transaction_size).min(transactions_per_interval);

        let mut counter = 0u64;
        let mut transactions_to_report = 0u64;
        let mut fractional_transactions = 0;
        let batch_size = transaction_size * transactions_per_interval;
        let mut buffer = vec![0u8; batch_size];

        // Cache the context clock at startup and derive subsequent
        // timestamps from a monotonic instant, avoiding repeated clock
        // syscalls in the hot loop. Works uniformly under TokioCtx
        // (real clock) and SimulatorContext (simulated clock).
        let base_system_time = C::timestamp_utc();
        let base_instant = C::now();

        let mut interval = C::interval(Self::TARGET_BLOCK_INTERVAL);
        C::sleep(config.initial_delay).await;
        loop {
            C::interval_tick(&mut interval).await;
            fractional_transactions += load % intervals_per_second;
            let transactions_this_interval =
                load / intervals_per_second + fractional_transactions / intervals_per_second;
            fractional_transactions %= intervals_per_second;
            if transactions_this_interval == 0 {
                continue;
            }
            buffer.resize(transaction_size * transactions_this_interval, 0);
            let timestamp_ms = (base_system_time + C::elapsed(&base_instant)).as_millis() as u64;

            let batch = Self::fill_batch(
                &mut buffer,
                transaction_size,
                transactions_this_interval,
                timestamp_ms,
                &mut counter,
                &mut random,
            );

            if !self
                .ship_blocks(
                    &batch,
                    transaction_size,
                    transactions_this_interval,
                    block_capacity,
                )
                .await
            {
                tracing::info!("Transaction channel closed, stopping generator");
                return;
            }

            transactions_to_report += transactions_this_interval as u64;
            if counter.is_multiple_of(100) {
                self.metrics
                    .inc_submitted_transactions(transactions_to_report);
                transactions_to_report = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dag::context::TokioCtx;
    use tokio::sync::mpsc;

    #[tokio::test(start_paused = true)]
    async fn sends_configured_number_of_transactions_per_second() {
        for load in [0, 1, 7, 10, 11, 19, 1001] {
            let (sender, mut receiver) = mpsc::channel(1024);
            let client = TransactionClient::new(sender);
            let config = LoadGeneratorConfig {
                load,
                ..LoadGeneratorConfig::new_for_test()
            };
            let handle = TransactionGenerator::start::<TokioCtx>(
                client,
                Authority::default(),
                config,
                1_000_000,
                Metrics::new_for_test(4),
            );
            let mut count = 0;
            for _ in 0..10 {
                tokio::task::yield_now().await;
                while let Ok(batch) = receiver.try_recv() {
                    count += batch.len();
                }
                tokio::time::advance(Duration::from_millis(100)).await;
            }
            handle.abort();
            let _ = handle.await;
            println!("RATE requested={load} observed_in_one_second={count}");
            assert_eq!(count, load, "incorrect transaction rate for load {load}");
        }
    }
}
