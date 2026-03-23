// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::SocketAddr,
    ops::AddAssign,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};

use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, CounterVec,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use tabled::{Table, Tabled};
use tokio::time::Instant;

use crate::{
    committee::Committee,
    data::{IN_MEMORY_BLOCKS, IN_MEMORY_BLOCKS_BYTES},
    runtime,
    stat::{histogram, DivUsize, HistogramSender, PreciseHistogram},
    types::{format_authority_index, AuthorityIndex},
};

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 0.75, 1., 1.25, 1.5, 1.75, 2., 2.5, 3.0, 4.0, 5., 10., 20., 30., 60., 90.,
];

/// Metrics collected by the benchmark.
pub const BENCHMARK_DURATION: &str = "benchmark_duration";
pub const LATENCY_S: &str = "latency_s";
pub const LATENCY_SQUARED_S: &str = "latency_squared_s";

pub struct Metrics {
    benchmark_duration: IntCounter,
    latency_s: HistogramVec,
    latency_squared_s: CounterVec,
    committed_leaders_total: IntCounterVec,
    leader_timeout_total: IntCounter,
    inter_block_latency_s: HistogramVec,

    block_store_unloaded_blocks: IntCounter,
    block_store_loaded_blocks: IntCounter,
    block_store_entries: IntCounter,
    block_store_cleanup_util: IntCounter,

    wal_mappings: IntGauge,

    core_lock_util: IntCounter,
    core_lock_enqueued: IntCounter,
    core_lock_dequeued: IntCounter,

    block_handler_pending_certificates: IntGauge,
    block_handler_cleanup_util: IntCounter,

    commit_handler_pending_certificates: IntGauge,

    missing_blocks: IntGaugeVec,
    block_sync_requests_sent: IntCounterVec,
    block_sync_requests_received: IntCounterVec,

    transaction_certified_latency: HistogramSender<Duration>,
    certificate_committed_latency: HistogramSender<Duration>,
    transaction_committed_latency: HistogramSender<Duration>,

    proposed_block_size_bytes: HistogramSender<usize>,
    proposed_block_transaction_count: HistogramSender<usize>,
    proposed_block_vote_count: HistogramSender<usize>,

    connection_latency_sender: Vec<HistogramSender<Duration>>,

    utilization_timer: IntCounterVec,
    submitted_transactions: IntCounter,

    /// Stored reporter for test-constructed metrics.
    /// `None` in production (reporter runs as background
    /// task). `Some` in tests (drained on-demand via
    /// `print_stats`).
    reporter: Mutex<Option<MetricReporter>>,
}

struct MetricReporter {
    // When adding field here make sure to update
    // MetricReporter::clear_receive_all and run_report.
    transaction_certified_latency: HistogramReporter<Duration>,
    certificate_committed_latency: HistogramReporter<Duration>,
    transaction_committed_latency: HistogramReporter<Duration>,

    proposed_block_size_bytes: HistogramReporter<usize>,
    proposed_block_transaction_count: HistogramReporter<usize>,
    proposed_block_vote_count: HistogramReporter<usize>,

    connection_latency: VecHistogramReporter<Duration>,

    global_in_memory_blocks: IntGauge,
    global_in_memory_blocks_bytes: IntGauge,
}

struct HistogramReporter<T> {
    histogram: PreciseHistogram<T>,
    gauge: IntGaugeVec,
}

struct VecHistogramReporter<T> {
    histograms: Vec<(PreciseHistogram<T>, String)>,
    gauge: IntGaugeVec,
}

// ── Constructors ────────────────────────────────────

impl Metrics {
    /// Create metrics and start the background reporter.
    pub fn new(registry: &Registry, committee: Option<&Committee>) -> Arc<Self> {
        let (metrics, reporter) = Self::build(registry, committee);
        reporter.start();
        metrics
    }

    /// Create metrics for tests. The reporter is stored
    /// internally and drained on-demand via `print_stats`.
    pub fn new_for_test(registry: &Registry, committee: Option<&Committee>) -> Arc<Self> {
        let (metrics, reporter) = Self::build(registry, committee);
        *metrics.reporter.lock().unwrap() = Some(reporter);
        metrics
    }

    fn build(registry: &Registry, committee: Option<&Committee>) -> (Arc<Self>, MetricReporter) {
        let (transaction_certified_latency_hist, transaction_certified_latency) = histogram();
        let (certificate_committed_latency_hist, certificate_committed_latency) = histogram();
        let (transaction_committed_latency_hist, transaction_committed_latency) = histogram();

        let (proposed_block_size_bytes_hist, proposed_block_size_bytes) = histogram();
        let (proposed_block_transaction_count_hist, proposed_block_transaction_count) = histogram();
        let (proposed_block_vote_count_hist, proposed_block_vote_count) = histogram();

        let committee_size = committee.map(Committee::len).unwrap_or_default();
        let (connection_latency_hist, connection_latency_sender) = (0..committee_size)
            .map(|peer| {
                let (hist, sender) = histogram();
                (
                    (
                        hist,
                        format_authority_index(peer as AuthorityIndex).to_string(),
                    ),
                    sender,
                )
            })
            .unzip();
        let reporter = MetricReporter {
            transaction_certified_latency: HistogramReporter::new_in_registry(
                transaction_certified_latency_hist,
                registry,
                "transaction_certified_latency",
            ),
            certificate_committed_latency: HistogramReporter::new_in_registry(
                certificate_committed_latency_hist,
                registry,
                "certificate_committed_latency",
            ),
            transaction_committed_latency: HistogramReporter::new_in_registry(
                transaction_committed_latency_hist,
                registry,
                "transaction_committed_latency",
            ),

            proposed_block_size_bytes: HistogramReporter::new_in_registry(
                proposed_block_size_bytes_hist,
                registry,
                "proposed_block_size_bytes",
            ),
            proposed_block_transaction_count: HistogramReporter::new_in_registry(
                proposed_block_transaction_count_hist,
                registry,
                "proposed_block_transaction_count",
            ),
            proposed_block_vote_count: HistogramReporter::new_in_registry(
                proposed_block_vote_count_hist,
                registry,
                "proposed_block_vote_count",
            ),

            connection_latency: VecHistogramReporter::new_in_registry(
                connection_latency_hist,
                "peer",
                registry,
                "connection_latency",
            ),

            global_in_memory_blocks: register_int_gauge_with_registry!(
                "global_in_memory_blocks",
                "Number of blocks loaded in memory",
                registry,
            )
            .unwrap(),
            global_in_memory_blocks_bytes: register_int_gauge_with_registry!(
                "global_in_memory_blocks_bytes",
                concat!("Total size of blocks loaded ", "in memory",),
                registry,
            )
            .unwrap(),
        };
        let metrics = Self {
            benchmark_duration: register_int_counter_with_registry!(
                BENCHMARK_DURATION,
                "Duration of the benchmark",
                registry,
            )
            .unwrap(),
            latency_s: register_histogram_vec_with_registry!(
                LATENCY_S,
                concat!(
                    "Buckets measuring the ",
                    "end-to-end latency of a ",
                    "workload in seconds",
                ),
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            latency_squared_s: register_counter_vec_with_registry!(
                LATENCY_SQUARED_S,
                concat!(
                    "Square of total end-to-end ",
                    "latency of a workload ",
                    "in seconds",
                ),
                &["workload"],
                registry,
            )
            .unwrap(),
            committed_leaders_total: register_int_counter_vec_with_registry!(
                "committed_leaders_total",
                concat!(
                    "Total number of (direct or ",
                    "indirect) committed leaders ",
                    "per authority",
                ),
                &["authority", "commit_type"],
                registry,
            )
            .unwrap(),
            inter_block_latency_s: register_histogram_vec_with_registry!(
                "inter_block_latency_s",
                concat!("Buckets measuring the ", "inter-block latency in seconds",),
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            submitted_transactions: register_int_counter_with_registry!(
                "submitted_transactions",
                concat!("Total number of submitted ", "transactions",),
                registry,
            )
            .unwrap(),
            leader_timeout_total: register_int_counter_with_registry!(
                "leader_timeout_total",
                "Total number of leader timeouts",
                registry,
            )
            .unwrap(),

            block_store_loaded_blocks: register_int_counter_with_registry!(
                "block_store_loaded_blocks",
                concat!("Blocks loaded from wal ", "position in the block store",),
                registry,
            )
            .unwrap(),
            block_store_unloaded_blocks: register_int_counter_with_registry!(
                "block_store_unloaded_blocks",
                concat!("Blocks unloaded from wal ", "position during cleanup",),
                registry,
            )
            .unwrap(),
            block_store_entries: register_int_counter_with_registry!(
                "block_store_entries",
                "Number of entries in block store",
                registry,
            )
            .unwrap(),
            block_store_cleanup_util: register_int_counter_with_registry!(
                "block_store_cleanup_util",
                "block_store_cleanup_util",
                registry,
            )
            .unwrap(),

            wal_mappings: register_int_gauge_with_registry!(
                "wal_mappings",
                concat!("Number of mappings retained ", "by the wal",),
                registry,
            )
            .unwrap(),

            core_lock_util: register_int_counter_with_registry!(
                "core_lock_util",
                "Utilization of core write lock",
                registry,
            )
            .unwrap(),
            core_lock_enqueued: register_int_counter_with_registry!(
                "core_lock_enqueued",
                concat!("Number of enqueued ", "core requests",),
                registry,
            )
            .unwrap(),
            core_lock_dequeued: register_int_counter_with_registry!(
                "core_lock_dequeued",
                concat!("Number of dequeued ", "core requests",),
                registry,
            )
            .unwrap(),

            block_handler_pending_certificates: register_int_gauge_with_registry!(
                "block_handler_pending_certificates",
                concat!("Number of pending certificates", " in block handler",),
                registry,
            )
            .unwrap(),
            block_handler_cleanup_util: register_int_counter_with_registry!(
                "block_handler_cleanup_util",
                "block_handler_cleanup_util",
                registry,
            )
            .unwrap(),

            commit_handler_pending_certificates: register_int_gauge_with_registry!(
                "commit_handler_pending_certificates",
                concat!("Number of pending certificates", " in commit handler",),
                registry,
            )
            .unwrap(),

            missing_blocks: register_int_gauge_vec_with_registry!(
                "missing_blocks",
                concat!("Number of missing blocks ", "per authority",),
                &["authority"],
                registry,
            )
            .unwrap(),
            block_sync_requests_sent: register_int_counter_vec_with_registry!(
                "block_sync_requests_sent",
                concat!("Number of block sync requests", " sent per authority",),
                &["authority"],
                registry,
            )
            .unwrap(),
            block_sync_requests_received: register_int_counter_vec_with_registry!(
                "block_sync_requests_received",
                concat!(
                    "Number of block sync requests",
                    " received per authority and ",
                    "whether they have been ",
                    "fulfilled",
                ),
                &["authority", "fulfilled"],
                registry,
            )
            .unwrap(),

            utilization_timer: register_int_counter_vec_with_registry!(
                "utilization_timer",
                "Utilization timer",
                &["proc"],
                registry,
            )
            .unwrap(),

            transaction_certified_latency,
            certificate_committed_latency,
            transaction_committed_latency,

            proposed_block_size_bytes,
            proposed_block_transaction_count,
            proposed_block_vote_count,

            connection_latency_sender,

            reporter: Mutex::new(None),
        };

        (Arc::new(metrics), reporter)
    }
}

// ── Delegation methods ──────────────────────────────

impl Metrics {
    // -- Simple counter increments --

    pub fn inc_leader_timeout(&self) {
        self.leader_timeout_total.inc();
    }

    pub fn inc_core_lock_enqueued(&self) {
        self.core_lock_enqueued.inc();
    }

    pub fn inc_core_lock_dequeued(&self) {
        self.core_lock_dequeued.inc();
    }

    pub fn inc_block_store_entries(&self) {
        self.block_store_entries.inc();
    }

    pub fn inc_block_store_entries_by(&self, n: u64) {
        self.block_store_entries.inc_by(n);
    }

    pub fn inc_block_store_loaded_blocks(&self) {
        self.block_store_loaded_blocks.inc();
    }

    pub fn inc_block_store_unloaded_blocks_by(&self, n: u64) {
        self.block_store_unloaded_blocks.inc_by(n);
    }

    pub fn inc_submitted_transactions(&self, n: u64) {
        self.submitted_transactions.inc_by(n);
    }

    pub fn inc_benchmark_duration_by(&self, delta: u64) {
        self.benchmark_duration.inc_by(delta);
    }

    // -- Gauge sets --

    pub fn set_wal_mappings(&self, value: i64) {
        self.wal_mappings.set(value);
    }

    pub fn set_block_handler_pending_certificates(&self, value: i64) {
        self.block_handler_pending_certificates.set(value);
    }

    pub fn set_commit_handler_pending_certificates(&self, value: i64) {
        self.commit_handler_pending_certificates.set(value);
    }

    // -- Counter reads --

    pub fn benchmark_duration_secs(&self) -> u64 {
        self.benchmark_duration.get()
    }

    // -- Channel-based observations --

    pub fn observe_transaction_certified_latency(&self, d: Duration) {
        self.transaction_certified_latency.observe(d);
    }

    pub fn observe_certificate_committed_latency(&self, d: Duration) {
        self.certificate_committed_latency.observe(d);
    }

    pub fn observe_transaction_committed_latency(&self, d: Duration) {
        self.transaction_committed_latency.observe(d);
    }

    pub fn observe_proposed_block_size_bytes(&self, size: usize) {
        self.proposed_block_size_bytes.observe(size);
    }

    pub fn observe_proposed_block_transaction_count(&self, count: usize) {
        self.proposed_block_transaction_count.observe(count);
    }

    pub fn observe_proposed_block_vote_count(&self, count: usize) {
        self.proposed_block_vote_count.observe(count);
    }

    // -- Labeled metrics --

    pub fn observe_latency_s(&self, workload: &str, value: f64) {
        self.latency_s.with_label_values(&[workload]).observe(value);
    }

    pub fn observe_latency_squared_s(&self, workload: &str, value: f64) {
        self.latency_squared_s
            .with_label_values(&[workload])
            .inc_by(value);
    }

    pub fn observe_inter_block_latency_s(&self, workload: &str, value: f64) {
        self.inter_block_latency_s
            .with_label_values(&[workload])
            .observe(value);
    }

    pub fn inc_committed_leaders(&self, authority: &str, commit_type: &str) {
        self.committed_leaders_total
            .with_label_values(&[authority, commit_type])
            .inc();
    }

    pub fn set_missing_blocks(&self, authority: &str, value: i64) {
        self.missing_blocks
            .with_label_values(&[authority])
            .set(value);
    }

    pub fn inc_block_sync_requests_sent(&self, authority: &str) {
        self.block_sync_requests_sent
            .with_label_values(&[authority])
            .inc();
    }

    pub fn inc_block_sync_requests_received(&self, authority: &str, fulfilled: &str) {
        self.block_sync_requests_received
            .with_label_values(&[authority, fulfilled])
            .inc();
    }

    // -- Network --

    pub fn connection_latency_sender(&self, peer: usize) -> HistogramSender<Duration> {
        self.connection_latency_sender
            .get(peer)
            .expect("Missing connection_latency_sender")
            .clone()
    }

    // -- Utilization timers --

    pub fn core_lock_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.core_lock_util,
            start: Instant::now(),
        }
    }

    pub fn block_store_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.block_store_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn block_handler_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.block_handler_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer {
        OwnedUtilizationTimer {
            metric: self.utilization_timer.with_label_values(&[label]),
            start: Instant::now(),
        }
    }

    // -- Test utilities --

    /// Drain channels and print latency percentiles.
    /// Only works for test-constructed metrics (where
    /// the reporter is stored internally).
    pub fn print_stats(&self, authority: AuthorityIndex) {
        let mut guard = self.reporter.lock().unwrap();
        let Some(r) = guard.as_mut() else {
            return;
        };
        r.clear_receive_all();
        eprintln!(
            "  {} || {:05} | {:05} || {:05} | {:05} \
            || {:05} | {:05} |",
            format_authority_index(authority),
            r.transaction_certified_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.transaction_certified_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
            r.certificate_committed_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.certificate_committed_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
            r.transaction_committed_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.transaction_committed_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
        );
    }
}

// ── Reporter ────────────────────────────────────────

trait AsPrometheusMetric {
    fn as_prometheus_metric(&self) -> i64;
}

impl<T: Ord + AddAssign + DivUsize + Copy + Default + AsPrometheusMetric> HistogramReporter<T> {
    pub fn new_in_registry(
        histogram: PreciseHistogram<T>,
        registry: &Registry,
        name: &str,
    ) -> Self {
        let gauge = register_int_gauge_vec_with_registry!(name, name, &["v"], registry).unwrap();
        Self { histogram, gauge }
    }

    pub fn report(&mut self) -> Option<()> {
        let [p50, p90, p99] = self.histogram.pcts([500, 900, 990])?;
        self.gauge
            .with_label_values(&["p50"])
            .set(p50.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p90"])
            .set(p90.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p99"])
            .set(p99.as_prometheus_metric());
        self.gauge
            .with_label_values(&["sum"])
            .set(self.histogram.total_sum().as_prometheus_metric());
        self.gauge
            .with_label_values(&["count"])
            .set(self.histogram.total_count() as i64);
        None
    }

    pub fn clear_receive_all(&mut self) {
        self.histogram.clear_receive_all();
    }
}

impl<T: Ord + AddAssign + DivUsize + Copy + Default + AsPrometheusMetric> VecHistogramReporter<T> {
    pub fn new_in_registry(
        histograms: Vec<(PreciseHistogram<T>, String)>,
        label: &str,
        registry: &Registry,
        name: &str,
    ) -> Self {
        let gauge =
            register_int_gauge_vec_with_registry!(name, name, &[label, "v"], registry).unwrap();
        Self { histograms, gauge }
    }

    pub fn report(&mut self) {
        for (histogram, label) in self.histograms.iter_mut() {
            let Some([p50, p90, p99]) = histogram.pcts([500, 900, 990]) else {
                continue;
            };
            self.gauge
                .with_label_values(&[label, "p50"])
                .set(p50.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "p90"])
                .set(p90.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "p99"])
                .set(p99.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "sum"])
                .set(histogram.total_sum().as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "count"])
                .set(histogram.total_count() as i64);
        }
    }

    pub fn clear_receive_all(&mut self) {
        self.histograms
            .iter_mut()
            .for_each(|(hist, _)| hist.clear_receive_all());
    }
}

impl AsPrometheusMetric for Duration {
    fn as_prometheus_metric(&self) -> i64 {
        self.as_micros() as i64
    }
}

impl AsPrometheusMetric for usize {
    fn as_prometheus_metric(&self) -> i64 {
        *self as i64
    }
}

impl MetricReporter {
    fn start(self) {
        runtime::Handle::current().spawn(self.run());
    }

    pub fn clear_receive_all(&mut self) {
        self.transaction_certified_latency.clear_receive_all();
        self.certificate_committed_latency.clear_receive_all();
        self.transaction_committed_latency.clear_receive_all();

        self.proposed_block_size_bytes.clear_receive_all();
        self.proposed_block_transaction_count.clear_receive_all();
        self.proposed_block_vote_count.clear_receive_all();

        self.connection_latency.clear_receive_all();
    }

    // todo - this task never stops
    async fn run(mut self) {
        const REPORT_INTERVAL: Duration = Duration::from_secs(60);
        let mut deadline = Instant::now();
        loop {
            deadline += REPORT_INTERVAL;
            tokio::time::sleep_until(deadline).await;
            self.run_report().await;
        }
    }

    async fn run_report(&mut self) {
        self.global_in_memory_blocks
            .set(IN_MEMORY_BLOCKS.load(Ordering::Relaxed) as i64);
        self.global_in_memory_blocks_bytes
            .set(IN_MEMORY_BLOCKS_BYTES.load(Ordering::Relaxed) as i64);

        self.clear_receive_all();

        self.transaction_certified_latency.report();
        self.certificate_committed_latency.report();
        self.transaction_committed_latency.report();

        self.proposed_block_size_bytes.report();
        self.proposed_block_transaction_count.report();
        self.proposed_block_vote_count.report();

        self.connection_latency.report();
    }
}

// ── Network address table ───────────────────────────

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

// ── Utilization timers ──────────────────────────────

pub trait UtilizationTimerExt {
    fn utilization_timer(&self) -> UtilizationTimer<'_>;
    fn owned_utilization_timer(&self) -> OwnedUtilizationTimer;
}

pub trait UtilizationTimerVecExt {
    fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer;
}

impl UtilizationTimerExt for IntCounter {
    fn utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: self,
            start: Instant::now(),
        }
    }

    fn owned_utilization_timer(&self) -> OwnedUtilizationTimer {
        OwnedUtilizationTimer {
            metric: self.clone(),
            start: Instant::now(),
        }
    }
}

impl UtilizationTimerVecExt for IntCounterVec {
    fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer {
        self.with_label_values(&[label]).owned_utilization_timer()
    }
}

pub struct UtilizationTimer<'a> {
    metric: &'a IntCounter,
    start: Instant,
}

pub struct OwnedUtilizationTimer {
    metric: IntCounter,
    start: Instant,
}

impl<'a> Drop for UtilizationTimer<'a> {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

impl Drop for OwnedUtilizationTimer {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

#[derive(Tabled)]
struct NetworkAddressTable {
    peer: char,
    address: String,
}
