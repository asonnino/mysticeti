// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod aggregate;
mod coarse;
mod histogram;
mod names;
mod precise;
mod result;
mod snapshot;
mod timers;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use ::prometheus::Registry;
use tabled::{Table, Tabled};
use tokio::time::Instant;

pub use self::aggregate::AggregateMetrics;
pub use self::names::{
    BENCHMARK_DURATION, BLOCK_SYNC_REQUESTS_SENT, LABEL_AUTHORITY, LATENCY_S, LATENCY_SQUARED_S,
    LEADER_TIMEOUT_TOTAL, SyncRequestFulfilled,
};
pub use self::result::{Outcome, RunKind, RunResult};
pub use self::snapshot::MetricsSnapshot;
pub use self::timers::{OwnedUtilizationTimer, UtilizationTimer};
use self::{
    coarse::CoarseMetrics,
    names::{
        COMMIT_TYPE_DIRECT_COMMIT, COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_INDIRECT_COMMIT,
        COMMIT_TYPE_INDIRECT_SKIP,
    },
    precise::PreciseMetrics,
};
use crate::{authority::Authority, consensus::LeaderStatus};

pub struct Metrics {
    coarse: CoarseMetrics,
    precise: PreciseMetrics,
    registry: Registry,
}

impl Metrics {
    /// Create metrics and start the background reporter. The registry clone is retained
    /// so [`Metrics::collect`] can produce a snapshot at any time (heartbeats during a
    /// run, final summary at shutdown). The precise metrics are flushed periodically by
    /// the background reporter, so a snapshot may lag by up to `report_interval`.
    pub fn new(
        registry: &Registry,
        committee_size: usize,
        report_interval: Option<Duration>,
    ) -> Arc<Self> {
        let coarse = CoarseMetrics::new(registry);
        let precise = PreciseMetrics::spawn(registry, committee_size, report_interval);
        Arc::new(Self {
            coarse,
            precise,
            registry: registry.clone(),
        })
    }

    /// Create metrics for tests. Owns a private registry; [`Metrics::collect`] flushes
    /// the precise channels synchronously before gathering, so snapshots are always
    /// up-to-date.
    pub fn new_for_test(committee_size: usize) -> Arc<Self> {
        let registry = Registry::new();
        let coarse = CoarseMetrics::new(&registry);
        let precise = PreciseMetrics::new_for_test(&registry, committee_size);
        Arc::new(Self {
            coarse,
            precise,
            registry,
        })
    }
}

impl Metrics {
    pub fn inc_leader_timeout(&self) {
        self.coarse.leader_timeout_total.inc();
    }

    pub fn inc_core_lock_enqueued(&self) {
        self.coarse.core_lock_enqueued.inc();
    }

    pub fn inc_core_lock_dequeued(&self) {
        self.coarse.core_lock_dequeued.inc();
    }

    pub fn inc_block_store_entries(&self) {
        self.coarse.block_store_entries.inc();
    }

    pub fn inc_block_store_entries_by(&self, n: u64) {
        self.coarse.block_store_entries.inc_by(n);
    }

    pub fn inc_block_store_loaded_blocks(&self) {
        self.coarse.block_store_loaded_blocks.inc();
    }

    pub fn inc_block_store_unloaded_blocks_by(&self, n: u64) {
        self.coarse.block_store_unloaded_blocks.inc_by(n);
    }

    pub fn inc_submitted_transactions(&self, n: u64) {
        self.coarse.submitted_transactions.inc_by(n);
    }

    pub fn inc_benchmark_duration_by(&self, delta: u64) {
        self.coarse.benchmark_duration.inc_by(delta);
    }

    pub fn set_wal_mappings(&self, value: i64) {
        self.coarse.wal_mappings.set(value);
    }

    pub fn benchmark_duration_secs(&self) -> u64 {
        self.coarse.benchmark_duration.get()
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

    pub fn observe_latency_s(&self, value: f64) {
        self.coarse.latency_s.observe(value);
    }

    pub fn observe_latency_squared_s(&self, value: f64) {
        self.coarse.latency_squared_s.inc_by(value);
    }

    pub fn observe_inter_block_latency_s(&self, value: f64) {
        self.coarse.inter_block_latency_s.observe(value);
    }

    /// Record a decided leader on `committed_leaders_total`. Silent no-op on
    /// `LeaderStatus::Undecided` — only decided statuses (commit or skip, direct or indirect)
    /// produce a metric increment.
    pub fn inc_decided_leaders(&self, status: &LeaderStatus) {
        let label = match status {
            LeaderStatus::DirectCommit(_) => COMMIT_TYPE_DIRECT_COMMIT,
            LeaderStatus::IndirectCommit(_) => COMMIT_TYPE_INDIRECT_COMMIT,
            LeaderStatus::DirectSkip(..) => COMMIT_TYPE_DIRECT_SKIP,
            LeaderStatus::IndirectSkip(..) => COMMIT_TYPE_INDIRECT_SKIP,
            LeaderStatus::Undecided(..) => return,
        };
        let authority = status.authority().to_string();
        self.coarse
            .committed_leaders_total
            .with_label_values(&[&authority, label])
            .inc();
    }

    pub fn set_missing_blocks(&self, authority: Authority, value: i64) {
        let label = authority.to_string();
        self.coarse
            .missing_blocks
            .with_label_values(&[&label])
            .set(value);
    }

    pub fn inc_block_sync_requests_sent(&self, authority: Authority) {
        let label = authority.to_string();
        self.coarse
            .block_sync_requests_sent
            .with_label_values(&[&label])
            .inc();
    }

    pub fn inc_block_sync_requests_received(
        &self,
        authority: Authority,
        fulfilled: SyncRequestFulfilled,
    ) {
        let label = authority.to_string();
        self.coarse
            .block_sync_requests_received
            .with_label_values(&[&label, fulfilled.as_label()])
            .inc();
    }

    pub fn observe_connection_latency(&self, peer: usize, d: Duration) {
        self.precise.observe_connection_latency(peer, d);
    }

    pub fn core_lock_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.core_lock_util,
            start: Instant::now(),
        }
    }

    pub fn block_store_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.block_store_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn block_handler_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.block_handler_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer {
        OwnedUtilizationTimer {
            metric: self.coarse.utilization_timer.with_label_values(&[label]),
            start: Instant::now(),
        }
    }

    /// Flush precise metrics to Prometheus gauges (in test mode only — see below) and
    /// return a snapshot of all gathered metrics.
    ///
    /// Expensive: `Registry::gather()` allocates many small structs (one `MetricFamily`
    /// per metric, one `Metric` per series, plus label-pair storage), so it's well into
    /// "do not call in tight loops" territory — heartbeats and end-of-run pulls are fine.
    ///
    /// In production mode (`Metrics::new`) the precise reporter runs as a background
    /// task that flushes periodically, so a snapshot here may lag by up to the
    /// `report_interval` passed to the constructor.
    pub fn collect(&self) -> MetricsSnapshot {
        self.precise.flush();
        MetricsSnapshot::new(self.registry.gather())
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

    use super::{Authority, Metrics};
    use crate::consensus::LeaderStatus;
    use crate::metrics::names::{
        COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_INDIRECT_SKIP, COMMITTED_LEADERS_TOTAL,
        LABEL_AUTHORITY, LABEL_COMMIT_TYPE,
    };

    #[test]
    fn new_for_test_collect_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_block_store_entries();
        metrics.inc_block_store_entries();
        metrics.inc_submitted_transactions(100);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.scalar_value("block_store_entries", &[]), 2.0);
        assert_eq!(snapshot.scalar_value("submitted_transactions", &[]), 100.0);
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
        let a = Authority::from(0_usize);
        let b = Authority::from(1_usize);
        let metrics = Metrics::new_for_test(4);
        metrics.inc_decided_leaders(&LeaderStatus::DirectSkip(a, 1));
        metrics.inc_decided_leaders(&LeaderStatus::DirectSkip(a, 2));
        metrics.inc_decided_leaders(&LeaderStatus::IndirectSkip(b, 1));
        metrics.set_missing_blocks(a, 3);
        metrics.inc_block_sync_requests_sent(a);
        let snapshot = metrics.collect();
        let authority_a = a.to_string();
        let authority_b = b.to_string();
        assert_eq!(
            snapshot.scalar_value(
                COMMITTED_LEADERS_TOTAL,
                &[
                    (LABEL_AUTHORITY, &authority_a),
                    (LABEL_COMMIT_TYPE, COMMIT_TYPE_DIRECT_SKIP),
                ]
            ),
            2.0
        );
        assert_eq!(
            snapshot.scalar_value(
                COMMITTED_LEADERS_TOTAL,
                &[
                    (LABEL_AUTHORITY, &authority_b),
                    (LABEL_COMMIT_TYPE, COMMIT_TYPE_INDIRECT_SKIP),
                ]
            ),
            1.0
        );
        assert_eq!(
            snapshot.scalar_value("missing_blocks", &[(LABEL_AUTHORITY, &authority_a)]),
            3.0
        );
        assert_eq!(
            snapshot.scalar_value(
                "block_sync_requests_sent",
                &[(LABEL_AUTHORITY, &authority_a)]
            ),
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
        let p50 = snapshot.scalar_value("transaction_committed_latency", &[("v", "p50")]);
        assert!(p50 > 0.0, "p50 should be populated after flush");
    }

    #[test]
    fn set_gauges_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.set_wal_mappings(42);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.scalar_value("wal_mappings", &[]), 42.0);
    }

    #[test]
    fn production_collect_returns_snapshot_via_external_registry() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let registry = prometheus::Registry::new();
        let metrics = Metrics::new(&registry, 4, None);
        metrics.set_wal_mappings(7);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.scalar_value("wal_mappings", &[]), 7.0);
    }
}
