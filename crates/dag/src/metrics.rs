// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod aggregate;
mod histogram;
mod precise;
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
use crate::authority::Authority;

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
            registry: None, // Not needed in production
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

    pub fn benchmark_duration_secs(&self) -> u64 {
        self.aggregate.benchmark_duration.get()
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
            peer: Authority::from(peer).to_string(),
            address: address.to_string(),
        })
        .collect();
    tracing::info!("Network address table:\n{}", Table::new(table));
}

#[derive(Tabled)]
struct NetworkAddressTable {
    peer: String,
    address: String,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::Metrics;

    #[test]
    fn new_for_test_collect_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_block_store_entries();
        metrics.inc_block_store_entries();
        metrics.inc_submitted_transactions(100);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.metric("block_store_entries", &[]), 2.0);
        assert_eq!(snapshot.metric("submitted_transactions", &[]), 100.0);
    }

    #[test]
    fn benchmark_duration_secs() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_benchmark_duration_by(10);
        assert_eq!(metrics.benchmark_duration_secs(), 10);
        metrics.inc_benchmark_duration_by(5);
        assert_eq!(metrics.benchmark_duration_secs(), 15);
    }

    #[test]
    fn labeled_metrics_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_committed_leaders("A", "direct");
        metrics.inc_committed_leaders("A", "direct");
        metrics.inc_committed_leaders("B", "indirect");
        metrics.set_missing_blocks("A", 3);
        metrics.inc_block_sync_requests_sent("A");
        let snapshot = metrics.collect();
        assert_eq!(
            snapshot.metric(
                "committed_leaders_total",
                &[("authority", "A"), ("commit_type", "direct")]
            ),
            2.0
        );
        assert_eq!(
            snapshot.metric(
                "committed_leaders_total",
                &[("authority", "B"), ("commit_type", "indirect")]
            ),
            1.0
        );
        assert_eq!(
            snapshot.metric("missing_blocks", &[("authority", "A")]),
            3.0,
        );
        assert_eq!(
            snapshot.metric("block_sync_requests_sent", &[("authority", "A")]),
            1.0
        );
    }

    #[test]
    fn observe_precise_metrics() {
        let metrics = Metrics::new_for_test(4);
        for i in 1..=100 {
            metrics.observe_transaction_committed_latency(Duration::from_micros(i));
        }
        let snapshot = metrics.collect();
        let p50 = snapshot.metric("transaction_committed_latency", &[("v", "p50")]);
        assert!(p50 > 0.0, "p50 should be populated after flush");
    }

    #[test]
    fn set_gauges_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.set_wal_mappings(42);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.metric("wal_mappings", &[]), 42.0);
    }

    #[test]
    #[should_panic(expected = "collect() is only available on test metrics")]
    fn collect_panics_without_registry() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let registry = prometheus::Registry::new();
        let metrics = Metrics::new(&registry, 4, None);
        metrics.collect();
    }
}
