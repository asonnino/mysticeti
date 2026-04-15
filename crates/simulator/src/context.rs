// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cell::RefCell, future::Future, pin::Pin, time::Duration};

use rand::prelude::StdRng;
use tokio::sync::{mpsc, oneshot};

use super::event_simulator::{Scheduler, simulator_time};
use super::executor::{ExecutorStateEvent, JoinError, JoinHandle, Sleep, simulator_spawn};
use dag::consensus::DagConsensus;
use dag::context::Ctx;
use dag::core::syncer::Syncer;
use dag::storage::WalSyncer;
use dag::types::AuthorityIndex;

use super::dispatcher::InlineDispatcher;

#[derive(Clone)]
pub struct SimulatorInstant(Duration);

pub(crate) struct Task {
    pub f: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub node: Option<AuthorityIndex>,
}

thread_local! {
    static CONTEXT: RefCell<Option<SimulatorContext>> = const { RefCell::new(None) };
}

pub struct SimulatorContext {
    pub(crate) spawned: Vec<Task>,
    task_id: usize,
    pub(crate) current_node: Option<AuthorityIndex>,
}

// SAFETY: SimulatorContext only lives in a thread-local and
// is never shared across threads.
unsafe impl Sync for SimulatorContext {}

impl SimulatorContext {
    pub fn new(task_id: usize, current_node: Option<AuthorityIndex>) -> Self {
        Self {
            spawned: Default::default(),
            task_id,
            current_node,
        }
    }

    pub fn task_id() -> usize {
        CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .expect("Not running in simulator context")
                .task_id
        })
    }

    pub fn current_node() -> Option<AuthorityIndex> {
        CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .expect("Not running in simulator context")
                .current_node
        })
    }

    pub fn enter(self) {
        CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            assert!(ctx.is_none(), "Can not re-enter simulator context");
            *ctx = Some(self);
        })
    }

    pub fn exit() -> Self {
        CONTEXT.with(|ctx| {
            ctx.borrow_mut()
                .take()
                .expect("Not running in simulator context - can not exit")
        })
    }

    pub fn with_rng<R, F: FnOnce(&mut StdRng) -> R>(f: F) -> R {
        Scheduler::<ExecutorStateEvent>::with_rng(f)
    }

    pub fn time() -> Duration {
        simulator_time()
    }
}

impl Ctx for SimulatorContext {
    type Instant = SimulatorInstant;
    type Interval = Duration;
    type JoinHandle<T: Send + 'static> = JoinHandle<T>;
    type JoinError = JoinError;

    fn timestamp_utc() -> Duration {
        Self::time()
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        Sleep::new(duration)
    }

    fn now() -> Self::Instant {
        SimulatorInstant(Self::time())
    }

    fn elapsed(instant: &Self::Instant) -> Duration {
        Self::time() - instant.0
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

    type Dispatcher<D: DagConsensus> = InlineDispatcher<D>;

    fn create_dispatcher<D: DagConsensus>(syncer: Syncer<Self, D>) -> Self::Dispatcher<D> {
        InlineDispatcher::new(syncer)
    }

    fn start_wal_syncer(_wal_syncer: WalSyncer, _stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        oneshot::channel().1
    }
}

pub struct NodeScope {
    previous_node: Option<AuthorityIndex>,
}

impl NodeScope {
    pub fn with<R>(node: Option<AuthorityIndex>, f: impl FnOnce() -> R) -> R {
        let scope = Self::enter(node);
        let result = f();
        drop(scope);
        result
    }

    fn enter(new: Option<AuthorityIndex>) -> Self {
        let previous_node = CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let ctx = ctx.as_mut().expect("Not running in simulator context");
            let previous_node = ctx.current_node;
            ctx.current_node = new;
            previous_node
        });
        Self { previous_node }
    }
}

impl Drop for NodeScope {
    fn drop(&mut self) {
        CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let ctx = ctx.as_mut().expect("Not running in simulator context");
            ctx.current_node = self.previous_node;
        });
    }
}
