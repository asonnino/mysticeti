// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Mutex, time::Duration};

use prometheus::Registry;

use super::histogram::{self, HistogramObserver, HistogramReporter, VecHistogramReporter};
use crate::{
    runtime::{self, JoinHandle},
    types::{format_authority_index, AuthorityIndex},
};

const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Precise metrics with exact percentile computation.
///
/// Owns the observer halves (for hot-path `observe` calls)
/// and a [`ReporterState`] that either runs as a background
/// task (production) or is available for on-demand drain
/// (tests).
pub(super) struct PreciseMetrics {
    observers: Observers,
    state: ReporterState,
}

impl PreciseMetrics {
    pub fn spawn(
        registry: &Registry,
        committee_size: usize,
        report_interval: Option<Duration>,
    ) -> Self {
        let (reporter, observers) = Self::create(registry, committee_size);
        let handle = runtime::Handle::current()
            .spawn(reporter.run(report_interval.unwrap_or(DEFAULT_REPORT_INTERVAL)));
        Self {
            observers,
            state: ReporterState::Spawned(handle),
        }
    }

    pub fn new_for_test(registry: &Registry, committee_size: usize) -> Self {
        let (reporter, observers) = Self::create(registry, committee_size);
        Self {
            observers,
            state: ReporterState::Test(Box::new(Mutex::new(reporter))),
        }
    }

    fn create(registry: &Registry, committee_size: usize) -> (MetricsReporter, Observers) {
        let (transaction_certified_latency_observer, transaction_certified_latency) =
            histogram::histogram(registry, "transaction_certified_latency");
        let (certificate_committed_latency_observer, certificate_committed_latency) =
            histogram::histogram(registry, "certificate_committed_latency");
        let (transaction_committed_latency_observer, transaction_committed_latency) =
            histogram::histogram(registry, "transaction_committed_latency");
        let (proposed_block_size_bytes_observer, proposed_block_size_bytes) =
            histogram::histogram(registry, "proposed_block_size_bytes");
        let (proposed_block_transaction_count_observer, proposed_block_transaction_count) =
            histogram::histogram(registry, "proposed_block_transaction_count");
        let (proposed_block_vote_count_observer, proposed_block_vote_count) =
            histogram::histogram(registry, "proposed_block_vote_count");

        let peer_labels =
            (0..committee_size).map(|p| format_authority_index(p as AuthorityIndex).to_string());
        let (connection_latency_observers, connection_latency) =
            histogram::vec_histogram(registry, "peer", "connection_latency", peer_labels);

        let reporter = MetricsReporter {
            transaction_certified_latency,
            certificate_committed_latency,
            transaction_committed_latency,
            proposed_block_size_bytes,
            proposed_block_transaction_count,
            proposed_block_vote_count,
            connection_latency,
        };

        let observers = Observers {
            transaction_certified_latency: transaction_certified_latency_observer,
            certificate_committed_latency: certificate_committed_latency_observer,
            transaction_committed_latency: transaction_committed_latency_observer,
            proposed_block_size_bytes: proposed_block_size_bytes_observer,
            proposed_block_transaction_count: proposed_block_transaction_count_observer,
            proposed_block_vote_count: proposed_block_vote_count_observer,
            connection_latency: connection_latency_observers,
        };

        (reporter, observers)
    }

    pub fn observe_transaction_certified_latency(&self, d: Duration) {
        self.observers.transaction_certified_latency.observe(d);
    }

    pub fn observe_certificate_committed_latency(&self, d: Duration) {
        self.observers.certificate_committed_latency.observe(d);
    }

    pub fn observe_transaction_committed_latency(&self, d: Duration) {
        self.observers.transaction_committed_latency.observe(d);
    }

    pub fn observe_proposed_block_size_bytes(&self, size: usize) {
        self.observers.proposed_block_size_bytes.observe(size);
    }

    pub fn observe_proposed_block_transaction_count(&self, count: usize) {
        self.observers
            .proposed_block_transaction_count
            .observe(count);
    }

    pub fn observe_proposed_block_vote_count(&self, count: usize) {
        self.observers.proposed_block_vote_count.observe(count);
    }

    pub fn observe_connection_latency(&self, peer: usize, d: Duration) {
        if let Some(sender) = self.observers.connection_latency.get(peer) {
            sender.observe(d);
        } else {
            debug_assert!(false, "connection latency metric missing for peer {peer}");
            tracing::warn!("Connection latency metric missing for peer {peer}");
        }
    }

    /// Drain channels and write percentiles to Prometheus
    /// gauges. Only works in test mode.
    pub fn flush(&self) {
        if let ReporterState::Test(mutex) = &self.state {
            let mut r = mutex.lock().expect("reporter lock poisoned");
            r.flush();
        }
    }

    /// Abort the background reporter task and drop all
    /// observers, preventing unbounded channel growth.
    pub fn shutdown(self) {
        if let ReporterState::Spawned(handle) = self.state {
            handle.abort();
        }
    }
}

/// Temporary struct to pass observer halves from `build` to `from_parts`.
struct Observers {
    transaction_certified_latency: HistogramObserver<Duration>,
    certificate_committed_latency: HistogramObserver<Duration>,
    transaction_committed_latency: HistogramObserver<Duration>,
    proposed_block_size_bytes: HistogramObserver<usize>,
    proposed_block_transaction_count: HistogramObserver<usize>,
    proposed_block_vote_count: HistogramObserver<usize>,
    connection_latency: Vec<HistogramObserver<Duration>>,
}

enum ReporterState {
    Spawned(JoinHandle<()>),
    Test(Box<Mutex<MetricsReporter>>),
}

struct MetricsReporter {
    transaction_certified_latency: HistogramReporter<Duration>,
    certificate_committed_latency: HistogramReporter<Duration>,
    transaction_committed_latency: HistogramReporter<Duration>,
    proposed_block_size_bytes: HistogramReporter<usize>,
    proposed_block_transaction_count: HistogramReporter<usize>,
    proposed_block_vote_count: HistogramReporter<usize>,
    connection_latency: VecHistogramReporter<Duration>,
}

impl MetricsReporter {
    /// Drain all channels, compute percentiles, and write
    /// to Prometheus gauges.
    fn flush(&mut self) {
        self.transaction_certified_latency.clear_receive_all();
        self.certificate_committed_latency.clear_receive_all();
        self.transaction_committed_latency.clear_receive_all();
        self.proposed_block_size_bytes.clear_receive_all();
        self.proposed_block_transaction_count.clear_receive_all();
        self.proposed_block_vote_count.clear_receive_all();
        self.connection_latency.clear_receive_all();

        self.transaction_certified_latency.report();
        self.certificate_committed_latency.report();
        self.transaction_committed_latency.report();
        self.proposed_block_size_bytes.report();
        self.proposed_block_transaction_count.report();
        self.proposed_block_vote_count.report();
        self.connection_latency.report();
    }

    async fn run(mut self, report_interval: Duration) {
        let mut interval = tokio::time::interval(report_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // first tick completes immediately
        loop {
            interval.tick().await;
            self.flush();
        }
    }
}
