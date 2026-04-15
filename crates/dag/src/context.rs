// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Debug, future::Future, time::Duration};

use tokio::sync::{mpsc, oneshot};

use crate::consensus::DagConsensus;
use crate::core::core_thread::{CoreDispatch, ThreadedDispatcher};
use crate::core::syncer::Syncer;
use crate::storage::WalSyncer;

pub trait Ctx: Sized + Send + Sync + 'static {
    fn timestamp_utc() -> Duration;
    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send;

    type Instant: Clone + Send + Sync + 'static;
    fn now() -> Self::Instant;
    fn elapsed(instant: &Self::Instant) -> Duration;

    type Interval: Send + 'static;
    fn interval(period: Duration) -> Self::Interval;
    fn interval_tick(interval: &mut Self::Interval) -> impl Future<Output = ()> + Send;

    type JoinHandle<T: Send + 'static>: Future<Output = Result<T, Self::JoinError>>
        + Send
        + Sync
        + Unpin;
    type JoinError: Send + Debug + 'static;
    fn spawn<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static)
    -> Self::JoinHandle<T>;
    fn abort<T: Send + 'static>(handle: &Self::JoinHandle<T>);

    type Dispatcher<D: DagConsensus>: CoreDispatch<Self, D>;
    fn create_dispatcher<D: DagConsensus>(syncer: Syncer<Self, D>) -> Self::Dispatcher<D>;

    fn start_wal_syncer(wal_syncer: WalSyncer, stop: mpsc::Sender<()>) -> oneshot::Receiver<()>;
}

pub struct TokioCtx;

impl Ctx for TokioCtx {
    type Instant = tokio::time::Instant;
    type Interval = tokio::time::Interval;
    type JoinHandle<T: Send + 'static> = tokio::task::JoinHandle<T>;
    type JoinError = tokio::task::JoinError;

    fn timestamp_utc() -> Duration {
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }

    fn now() -> Self::Instant {
        tokio::time::Instant::now()
    }

    fn elapsed(instant: &Self::Instant) -> Duration {
        instant.elapsed()
    }

    fn interval(period: Duration) -> Self::Interval {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval
    }

    async fn interval_tick(interval: &mut Self::Interval) {
        interval.tick().await;
    }

    fn spawn<T: Send + 'static>(
        f: impl Future<Output = T> + Send + 'static,
    ) -> Self::JoinHandle<T> {
        tokio::task::spawn(f)
    }

    fn abort<T: Send + 'static>(handle: &Self::JoinHandle<T>) {
        handle.abort();
    }

    type Dispatcher<D: DagConsensus> = ThreadedDispatcher<Self, D>;

    fn create_dispatcher<D: DagConsensus>(syncer: Syncer<Self, D>) -> Self::Dispatcher<D> {
        ThreadedDispatcher::new(syncer)
    }

    fn start_wal_syncer(wal_syncer: WalSyncer, stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        wal_syncer_tokio::start(wal_syncer, stop)
    }
}

mod wal_syncer_tokio {
    use std::time::Duration;

    use tokio::{
        select,
        sync::{mpsc, oneshot},
    };

    use crate::storage::WalSyncer;

    struct AsyncWalSyncer {
        wal_syncer: WalSyncer,
        stop: mpsc::Sender<()>,
        _sender: oneshot::Sender<()>,
        runtime: tokio::runtime::Handle,
    }

    pub(super) fn start(wal_syncer: WalSyncer, stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let syncer = AsyncWalSyncer {
            wal_syncer,
            stop,
            _sender: sender,
            runtime: tokio::runtime::Handle::current(),
        };
        std::thread::Builder::new()
            .name("wal-syncer".to_string())
            .spawn(move || syncer.run())
            .expect("Failed to spawn wal-syncer");
        receiver
    }

    impl AsyncWalSyncer {
        fn run(mut self) {
            let runtime = self.runtime.clone();
            loop {
                if runtime.block_on(self.wait_next()) {
                    return;
                }
                self.wal_syncer.sync().expect("Failed to sync wal");
            }
        }

        async fn wait_next(&mut self) -> bool {
            select! {
                _wait = tokio::time::sleep(Duration::from_secs(1)) => {
                    false
                }
                _signal = self.stop.send(()) => {
                    true
                }
            }
        }
    }
}
