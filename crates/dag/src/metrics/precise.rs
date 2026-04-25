// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Mutex, time::Duration};

use prometheus::Registry;
use tokio::sync::{mpsc, oneshot};

use super::histogram::{self, HistogramObserver, HistogramReporter, VecHistogramReporter};
use crate::authority::Authority;

const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Capacity of the foreground → background flush-request channel. A small
/// buffer is enough: each request carries its own oneshot reply, and multiple
/// concurrent requesters serialise naturally on the receiver side.
const FLUSH_CHANNEL_CAPACITY: usize = 4;

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
        let (flush_tx, flush_rx) = mpsc::channel(FLUSH_CHANNEL_CAPACITY);
        let handle = tokio::runtime::Handle::current()
            .spawn(reporter.run(report_interval.unwrap_or(DEFAULT_REPORT_INTERVAL), flush_rx));
        Self {
            observers,
            state: ReporterState::Spawned { handle, flush_tx },
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
        let (transaction_committed_latency_observer, transaction_committed_latency) =
            histogram::histogram(registry, "transaction_committed_latency");
        let (proposed_block_size_bytes_observer, proposed_block_size_bytes) =
            histogram::histogram(registry, "proposed_block_size_bytes");
        let (proposed_block_transaction_count_observer, proposed_block_transaction_count) =
            histogram::histogram(registry, "proposed_block_transaction_count");
        let (proposed_block_vote_count_observer, proposed_block_vote_count) =
            histogram::histogram(registry, "proposed_block_vote_count");

        let peer_labels = (0..committee_size).map(|p| Authority::from(p).to_string());
        let (connection_latency_observers, connection_latency) =
            histogram::vec_histogram(registry, "peer", "connection_latency", peer_labels);

        let reporter = MetricsReporter {
            transaction_committed_latency,
            proposed_block_size_bytes,
            proposed_block_transaction_count,
            proposed_block_vote_count,
            connection_latency,
        };

        let observers = Observers {
            transaction_committed_latency: transaction_committed_latency_observer,
            proposed_block_size_bytes: proposed_block_size_bytes_observer,
            proposed_block_transaction_count: proposed_block_transaction_count_observer,
            proposed_block_vote_count: proposed_block_vote_count_observer,
            connection_latency: connection_latency_observers,
        };

        (reporter, observers)
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

    /// Drain channels and write percentiles to Prometheus gauges. In test mode
    /// this locks the reporter and flushes synchronously; in production mode
    /// it asks the background task to flush and waits for the reply, so the
    /// caller is guaranteed to see fresh gauges on return. The hot observer
    /// path is unaffected — only foreground callers (e.g. `Metrics::collect`)
    /// pay the round-trip cost.
    pub async fn flush(&self) {
        match &self.state {
            ReporterState::Test(mutex) => {
                let mut r = mutex.lock().expect("reporter lock poisoned");
                r.flush();
            }
            ReporterState::Spawned { flush_tx, .. } => {
                let (reply_tx, reply_rx) = oneshot::channel();
                // If `send` fails the background task is gone and the channel is
                // closed; nothing left to flush. If the task is dropped between
                // `send` and `reply_rx.await`, the receiver also returns Err —
                // either way we just return without ceremony.
                if flush_tx.send(reply_tx).await.is_ok() {
                    let _ = reply_rx.await;
                }
            }
        }
    }

    /// Abort the background reporter task and drop all
    /// observers, preventing unbounded channel growth.
    pub fn shutdown(self) {
        if let ReporterState::Spawned { handle, .. } = self.state {
            handle.abort();
        }
    }
}

/// Temporary struct to pass observer halves from `build` to `from_parts`.
struct Observers {
    transaction_committed_latency: HistogramObserver<Duration>,
    proposed_block_size_bytes: HistogramObserver<usize>,
    proposed_block_transaction_count: HistogramObserver<usize>,
    proposed_block_vote_count: HistogramObserver<usize>,
    connection_latency: Vec<HistogramObserver<Duration>>,
}

enum ReporterState {
    /// Production: the reporter is owned by a background task that flushes
    /// periodically. Foreground callers ask the task to flush by sending a
    /// oneshot reply over `flush_tx` and awaiting the reply.
    Spawned {
        handle: tokio::task::JoinHandle<()>,
        flush_tx: mpsc::Sender<oneshot::Sender<()>>,
    },
    /// Test: no background task. The reporter sits behind a mutex that the
    /// foreground locks directly — no concurrent access in this mode, so the
    /// lock is uncontended.
    Test(Box<Mutex<MetricsReporter>>),
}

struct MetricsReporter {
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
        self.transaction_committed_latency.clear_receive_all();
        self.proposed_block_size_bytes.clear_receive_all();
        self.proposed_block_transaction_count.clear_receive_all();
        self.proposed_block_vote_count.clear_receive_all();
        self.connection_latency.clear_receive_all();

        self.transaction_committed_latency.report();
        self.proposed_block_size_bytes.report();
        self.proposed_block_transaction_count.report();
        self.proposed_block_vote_count.report();
        self.connection_latency.report();
    }

    async fn run(
        mut self,
        report_interval: Duration,
        mut flush_rx: mpsc::Receiver<oneshot::Sender<()>>,
    ) {
        let mut interval = tokio::time::interval(report_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // first tick completes immediately
        loop {
            tokio::select! {
                _ = interval.tick() => self.flush(),
                request = flush_rx.recv() => match request {
                    Some(reply) => {
                        self.flush();
                        // The caller may have given up; that's fine.
                        let _ = reply.send(());
                    }
                    // All `Sender` clones dropped — `PreciseMetrics` is gone.
                    None => break,
                },
            }
        }
    }
}
