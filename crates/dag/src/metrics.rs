// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod aggregate;
mod histogram;
mod precise;
pub mod server;
mod snapshot;
mod timers;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use ::prometheus::Registry;
use tabled::{Table, Tabled};
use tokio::time::Instant;

pub use self::aggregate::{BENCHMARK_DURATION, LATENCY_S, LATENCY_SQUARED_S};
pub use self::snapshot::MetricsSnapshot;
pub use self::timers::{OwnedUtilizationTimer, UtilizationTimer};
use self::{aggregate::AggregateMetrics, precise::PreciseMetrics};
use crate::types::{format_authority_index, AuthorityIndex};

pub struct Metrics {
    aggregate: AggregateMetrics,
    precise: PreciseMetrics,
    registry: Option<Registry>,
}

impl Metrics {
    /// Create metrics and start the background reporter.
    pub fn new(
        registry: &Registry,
        committee_size: usize,
        report_interval: Option<Duration>,
    ) -> Arc<Self> {
        let aggregate = AggregateMetrics::new(registry);
        let precise = PreciseMetrics::spawn(registry, committee_size, report_interval);
        Arc::new(Self {
            aggregate,
            precise,
            registry: None,
        })
    }

    /// Create metrics for tests. The reporter is stored
    /// internally and drained on-demand via `print_stats`.
    pub fn new_for_test(committee_size: usize) -> Arc<Self> {
        let registry = Registry::new();
        let aggregate = AggregateMetrics::new(&registry);
        let precise = PreciseMetrics::new_for_test(&registry, committee_size);
        Arc::new(Self {
            aggregate,
            precise,
            registry: Some(registry),
        })
    }
}

impl Metrics {
    pub fn inc_leader_timeout(&self) {
        self.aggregate.leader_timeout_total.inc();
    }

    pub fn inc_core_lock_enqueued(&self) {
        self.aggregate.core_lock_enqueued.inc();
    }

    pub fn inc_core_lock_dequeued(&self) {
        self.aggregate.core_lock_dequeued.inc();
    }

    pub fn inc_block_store_entries(&self) {
        self.aggregate.block_store_entries.inc();
    }

    pub fn inc_block_store_entries_by(&self, n: u64) {
        self.aggregate.block_store_entries.inc_by(n);
    }

    pub fn inc_block_store_loaded_blocks(&self) {
        self.aggregate.block_store_loaded_blocks.inc();
    }

    pub fn inc_block_store_unloaded_blocks_by(&self, n: u64) {
        self.aggregate.block_store_unloaded_blocks.inc_by(n);
    }

    pub fn inc_submitted_transactions(&self, n: u64) {
        self.aggregate.submitted_transactions.inc_by(n);
    }

    pub fn inc_benchmark_duration_by(&self, delta: u64) {
        self.aggregate.benchmark_duration.inc_by(delta);
    }

    pub fn set_wal_mappings(&self, value: i64) {
        self.aggregate.wal_mappings.set(value);
    }

    pub fn set_block_handler_pending_certificates(&self, value: i64) {
        self.aggregate.block_handler_pending_certificates.set(value);
    }

    pub fn set_commit_handler_pending_certificates(&self, value: i64) {
        self.aggregate
            .commit_handler_pending_certificates
            .set(value);
    }

    pub fn benchmark_duration_secs(&self) -> u64 {
        self.aggregate.benchmark_duration.get()
    }

    pub fn observe_transaction_certified_latency(&self, d: Duration) {
        self.precise.observe_transaction_certified_latency(d);
    }

    pub fn observe_certificate_committed_latency(&self, d: Duration) {
        self.precise.observe_certificate_committed_latency(d);
    }

    pub fn observe_transaction_committed_latency(&self, d: Duration) {
        self.precise.observe_transaction_committed_latency(d);
    }

    pub fn observe_proposed_block_size_bytes(&self, size: usize) {
        self.precise.observe_proposed_block_size_bytes(size);
    }

    pub fn observe_proposed_block_transaction_count(&self, count: usize) {
        self.precise.observe_proposed_block_transaction_count(count);
    }

    pub fn observe_proposed_block_vote_count(&self, count: usize) {
        self.precise.observe_proposed_block_vote_count(count);
    }

    pub fn observe_latency_s(&self, workload: &str, value: f64) {
        self.aggregate
            .latency_s
            .with_label_values(&[workload])
            .observe(value);
    }

    pub fn observe_latency_squared_s(&self, workload: &str, value: f64) {
        self.aggregate
            .latency_squared_s
            .with_label_values(&[workload])
            .inc_by(value);
    }

    pub fn observe_inter_block_latency_s(&self, workload: &str, value: f64) {
        self.aggregate
            .inter_block_latency_s
            .with_label_values(&[workload])
            .observe(value);
    }

    pub fn inc_committed_leaders(&self, authority: &str, commit_type: &str) {
        self.aggregate
            .committed_leaders_total
            .with_label_values(&[authority, commit_type])
            .inc();
    }

    pub fn set_missing_blocks(&self, authority: &str, value: i64) {
        self.aggregate
            .missing_blocks
            .with_label_values(&[authority])
            .set(value);
    }

    pub fn inc_block_sync_requests_sent(&self, authority: &str) {
        self.aggregate
            .block_sync_requests_sent
            .with_label_values(&[authority])
            .inc();
    }

    pub fn inc_block_sync_requests_received(&self, authority: &str, fulfilled: &str) {
        self.aggregate
            .block_sync_requests_received
            .with_label_values(&[authority, fulfilled])
            .inc();
    }

    pub fn observe_connection_latency(&self, peer: usize, d: Duration) {
        self.precise.observe_connection_latency(peer, d);
    }

    pub fn core_lock_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.aggregate.core_lock_util,
            start: Instant::now(),
        }
    }

    pub fn block_store_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.aggregate.block_store_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn block_handler_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.aggregate.block_handler_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer {
        OwnedUtilizationTimer {
            metric: self.aggregate.utilization_timer.with_label_values(&[label]),
            start: Instant::now(),
        }
    }

    /// Flush precise metrics to Prometheus gauges and return
    /// a snapshot of all metrics. Only works in test mode —
    /// panics if called on production metrics.
    pub fn collect(&self) -> MetricsSnapshot {
        let registry = self
            .registry
            .as_ref()
            .expect("collect() is only available on test metrics");
        self.precise.flush();
        MetricsSnapshot::from_families(registry.gather())
    }

    /// Abort the background reporter and drop all observers.
    /// Consumes self to prevent use after shutdown.
    pub fn shutdown(self) {
        self.precise.shutdown();
    }
}

pub fn print_network_address_table(addresses: &[SocketAddr]) {
    let table: Vec<_> = addresses
        .iter()
        .enumerate()
        .map(|(peer, address)| NetworkAddressTable {
            peer: format_authority_index(peer as AuthorityIndex),
            address: address.to_string(),
        })
        .collect();
    tracing::info!("Network address table:\n{}", Table::new(table));
}

#[derive(Tabled)]
struct NetworkAddressTable {
    peer: char,
    address: String,
}
