// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake},
    time::Duration,
};

use futures::FutureExt;
use rand::prelude::StdRng;
use tokio::{
    select,
    sync::{Notify, oneshot},
};

use super::context::{SimulatorContext, Task};
use super::event_simulator::{Scheduler, Simulator, SimulatorState};

#[derive(Default)]
pub struct SimulatorExecutor {
    tasks: HashMap<usize, Task>,
    next_id: usize,
}

pub struct JoinHandle<R> {
    ch: Pin<Box<oneshot::Receiver<Result<R, JoinError>>>>,
    abort: Arc<Notify>,
}

impl SimulatorExecutor {
    pub fn run<R: Send + 'static, F: Future<Output = R> + Send + 'static>(rng: StdRng, f: F) -> R {
        let mut simulator = Self::new_simulator(rng);
        Self::block_on(&mut simulator, f)
    }

    fn new_simulator(rng: StdRng) -> Simulator<SimulatorExecutor> {
        Simulator::new(vec![Self::default()], rng)
    }

    fn spawn_at<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
        simulator: &mut Simulator<SimulatorExecutor>,
        f: F,
    ) -> JoinHandle<R> {
        let state = &mut simulator.states_mut()[0];
        let (task, join_handle) = make_task(f);
        let task = Task {
            f: task,
            node: None,
        };
        let task_id = state.create_task(task);
        simulator.schedule_event(
            Duration::from_nanos(1),
            0,
            ExecutorStateEvent::Wake(task_id),
        );
        join_handle
    }

    fn block_on<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
        simulator: &mut Simulator<SimulatorExecutor>,
        f: F,
    ) -> R {
        let jh = Self::spawn_at(simulator, f);
        Self::run_until_complete(simulator, jh)
    }

    fn run_until_complete<R: Send + 'static>(
        simulator: &mut Simulator<SimulatorExecutor>,
        mut jh: JoinHandle<R>,
    ) -> R {
        loop {
            assert!(!simulator.run_one());
            match jh.check_complete() {
                Ok(value) => break value,
                Err(njh) => jh = njh,
            }
        }
    }

    fn create_task(&mut self, task: Task) -> usize {
        let task_id = self.next_id;
        self.next_id += 1;
        let p = self.tasks.insert(task_id, task);
        assert!(p.is_none());
        task_id
    }
}

pub fn simulator_spawn<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
    f: F,
) -> JoinHandle<R> {
    let mut context = SimulatorContext::exit();
    let (task, join_handle) = make_task(f);
    let task = Task {
        f: task,
        node: context.current_node,
    };
    context.spawned.push(task);
    context.enter();
    join_handle
}

fn make_task<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
    f: F,
) -> (Pin<Box<dyn Future<Output = ()> + Send>>, JoinHandle<R>) {
    let (s, r) = oneshot::channel();
    let abort = Arc::new(Notify::new());
    let task = task(f, s, abort.clone());
    let join_handle = JoinHandle {
        ch: Box::pin(r),
        abort,
    };
    let task = task.boxed();
    (task, join_handle)
}

#[derive(Debug)]
pub struct JoinError;

async fn task<F: Future>(
    f: F,
    ch: oneshot::Sender<Result<F::Output, JoinError>>,
    abort: Arc<Notify>,
) {
    select! {
        r = f => {
            ch.send(Ok(r)).ok();
        }
        _ = abort.notified() => {
            ch.send(Err(JoinError)).ok();
        }
    }
}

impl<R> JoinHandle<R> {
    pub fn abort(&self) {
        self.abort.notify_one();
    }

    fn check_complete(mut self) -> Result<R, Self> {
        match self.ch.try_recv() {
            Ok(Ok(value)) => Ok(value),
            _ => Err(self),
        }
    }
}

impl<R> Future for JoinHandle<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ch.as_mut().poll(cx).map(|res| match res {
            Ok(res) => res,
            Err(_) => Err(JoinError),
        })
    }
}

pub enum ExecutorStateEvent {
    Wake(usize),
}

impl SimulatorState for SimulatorExecutor {
    type Event = ExecutorStateEvent;

    fn handle_event(&mut self, event: Self::Event) {
        match event {
            ExecutorStateEvent::Wake(task_id) => {
                let Entry::Occupied(mut oc) = self.tasks.entry(task_id) else {
                    return;
                };
                let waker = Arc::new(Waker(task_id));
                let waker = waker.into();
                let mut context = Context::from_waker(&waker);
                let task = oc.get_mut();
                SimulatorContext::new(task_id, task.node).enter();
                if let Poll::Ready(()) = task.f.as_mut().poll(&mut context) {
                    oc.remove();
                }
                let context = SimulatorContext::exit();
                for task in context.spawned {
                    let id = self.create_task(task);
                    Scheduler::schedule_event(
                        Duration::from_nanos(id as u64),
                        0,
                        ExecutorStateEvent::Wake(id),
                    );
                }
            }
        }
    }
}

struct Waker(usize);

impl Wake for Waker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Scheduler::schedule_event(
            Duration::from_nanos(self.0 as u64),
            0,
            ExecutorStateEvent::Wake(self.0),
        );
    }
}

pub enum Sleep {
    Created(Duration),
    WaitingUntil(Duration),
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self::Created(duration)
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.get_mut();
        match ptr {
            Sleep::Created(duration) => {
                let ready = Scheduler::schedule_event(
                    *duration,
                    0,
                    ExecutorStateEvent::Wake(SimulatorContext::task_id()),
                );
                *ptr = Sleep::WaitingUntil(ready);
                Poll::Pending
            }
            Sleep::WaitingUntil(deadline) => {
                if super::event_simulator::simulator_time() >= *deadline {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
