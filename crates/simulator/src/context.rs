// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, time::Duration};

use tokio::sync::{mpsc, oneshot};

use super::executor::{simulator_spawn, JoinError, JoinHandle, SimulatorContext, Sleep};
use dag::context::Ctx;
use dag::storage::WalSyncer;

pub struct SimulatedCtx;

#[derive(Clone)]
pub struct SimInstant(Duration);

impl Ctx for SimulatedCtx {
    type Instant = SimInstant;
    type Interval = Duration;
    type JoinHandle<T: Send + 'static> = JoinHandle<T>;
    type JoinError = JoinError;

    fn timestamp_utc() -> Duration {
        SimulatorContext::time()
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        Sleep::new(duration)
    }

    fn now() -> Self::Instant {
        SimInstant(SimulatorContext::time())
    }

    fn elapsed(instant: &Self::Instant) -> Duration {
        SimulatorContext::time() - instant.0
    }

    fn interval(period: Duration) -> Self::Interval {
        period
    }

    fn interval_tick(interval: &mut Self::Interval) -> impl Future<Output = ()> + Send {
        Sleep::new(*interval)
    }

    fn spawn<T: Send + 'static>(
        f: impl Future<Output = T> + Send + 'static,
    ) -> Self::JoinHandle<T> {
        simulator_spawn(f)
    }

    fn abort<T: Send + 'static>(handle: &Self::JoinHandle<T>) {
        handle.abort();
    }

    fn start_wal_syncer(
        _wal_syncer: WalSyncer,
        _stop: mpsc::Sender<()>,
        _epoch_signal: mpsc::Sender<()>,
    ) -> oneshot::Receiver<()> {
        oneshot::channel().1
    }
}
