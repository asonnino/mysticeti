// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    any::Any,
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::BinaryHeap,
    time::Duration,
};

use rand::prelude::StdRng;

#[cfg(test)]
use rand::SeedableRng;

pub struct Simulator<S: SimulatorState>
where
    S::Event: 'static,
{
    states: Vec<S>,
    time: Duration,
    events: BinaryHeap<ScheduledEvent<S::Event>>,
    /// Sequence number of the next scheduled event; breaks ties between events due at the
    /// same time in scheduling order.
    next_sequence: u64,
    rng: Option<StdRng>,
}

pub struct Scheduler<E> {
    /// Events scheduled while handling one event, in scheduling order: `(time, state, event)`.
    events: Vec<(Duration, usize, E)>,
    rng: StdRng,
    time: Duration,
}

pub trait SimulatorState {
    type Event;

    fn handle_event(&mut self, event: Self::Event);
}

impl<S: SimulatorState + 'static> Simulator<S>
where
    S::Event: 'static,
{
    pub fn new(states: Vec<S>, rng: StdRng) -> Self {
        Self {
            states,
            time: Default::default(),
            events: Default::default(),
            next_sequence: 0,
            rng: Some(rng),
        }
    }

    pub fn schedule_event(&mut self, after: Duration, state: usize, event: S::Event) {
        self.push_event(self.time.saturating_add(after), state, event);
    }

    fn push_event(&mut self, time: Duration, state: usize, event: S::Event) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.events.push(ScheduledEvent {
            time,
            sequence,
            state,
            event,
        });
    }

    /// returns true if complete
    pub fn run_one(&mut self) -> bool {
        if let Some(event) = self.events.pop() {
            self.time = event.time;
            SIMULATOR_TIME.with(|cell| cell.set(self.time));
            self.run_event(event.state, event.event);
        }
        self.events.is_empty()
    }

    pub fn states(&self) -> &[S] {
        &self.states
    }

    #[allow(dead_code)]
    pub fn states_mut(&mut self) -> &mut [S] {
        &mut self.states
    }

    pub fn time(&self) -> Duration {
        self.time
    }

    fn run_event(&mut self, state: usize, event: S::Event) {
        Scheduler::<S::Event>::enter(self.time, self.rng.take().unwrap());
        let guard = SchedulerEnterGuard { simulator: self };
        let state = &mut guard.simulator.states[state];
        state.handle_event(event);
    }
}

struct SchedulerEnterGuard<'a, E: SimulatorState + 'static> {
    simulator: &'a mut Simulator<E>,
}

impl<'a, E: SimulatorState + 'static> Drop for SchedulerEnterGuard<'a, E> {
    fn drop(&mut self) {
        let (events, rng) = Scheduler::exit();
        self.simulator.rng = Some(rng);
        for (time, state, event) in events {
            self.simulator.push_event(time, state, event);
        }
    }
}

thread_local! {
    static SCHEDULER: RefCell<Option<Box<dyn Any>>> = RefCell::new(None);
    static SIMULATOR_TIME: Cell<Duration> = const {Cell::new(Duration::ZERO)};
}

pub fn simulator_time() -> Duration {
    SIMULATOR_TIME.with(|cell| cell.get())
}

impl<E: 'static> Scheduler<E> {
    pub fn schedule_event(after: Duration, state: usize, event: E) -> Duration {
        Self::with(|scheduler| {
            let time = scheduler.time.saturating_add(after);
            scheduler.events.push((time, state, event));
            time
        })
    }

    pub fn with_rng<R, F: FnOnce(&mut StdRng) -> R>(f: F) -> R {
        Self::with(|scheduler| f(&mut scheduler.rng))
    }

    fn with<R, F: FnOnce(&mut Self) -> R>(f: F) -> R {
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            let o = o.as_mut().unwrap();
            f(o.downcast_mut().unwrap())
        })
    }

    fn enter(time: Duration, rng: StdRng) {
        let scheduler = Self {
            time,
            rng,
            events: vec![],
        };
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            assert!(o.is_none());
            *o = Some(Box::new(scheduler));
        });
    }

    fn exit() -> (Vec<(Duration, usize, E)>, StdRng) {
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            let scheduler = o.take().unwrap();
            let scheduler: Box<Scheduler<E>> = scheduler.downcast().unwrap();
            let scheduler = *scheduler;
            (scheduler.events, scheduler.rng)
        })
    }
}

impl<S: SimulatorState> Drop for Simulator<S>
where
    S::Event: 'static,
{
    fn drop(&mut self) {
        Scheduler::<S::Event>::enter(self.time, self.rng.take().unwrap());
        self.states.clear();
        Scheduler::<S::Event>::exit();
        SIMULATOR_TIME.with(|cell| cell.set(Duration::ZERO));
    }
}

struct ScheduledEvent<E> {
    time: Duration,
    sequence: u64,
    state: usize,
    event: E,
}

impl<E> PartialOrd for ScheduledEvent<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> PartialEq for ScheduledEvent<E> {
    fn eq(&self, other: &Self) -> bool {
        (self.time, self.sequence) == (other.time, other.sequence)
    }
}

/// Reversed, so that the max-heap pops the earliest event first, and the first scheduled
/// among events due at the same time.
impl<E> Ord for ScheduledEvent<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.sequence).cmp(&(self.time, self.sequence))
    }
}

impl<E> Eq for ScheduledEvent<E> {}

#[cfg(test)]
mod test {
    use super::*;

    impl SimulatorState for u64 {
        type Event = u64;

        fn handle_event(&mut self, event: Self::Event) {
            *self += event;
        }
    }

    impl SimulatorState for Vec<u64> {
        type Event = u64;

        fn handle_event(&mut self, event: Self::Event) {
            self.push(event);
        }
    }

    #[test]
    fn same_time_events_run_in_scheduling_order() {
        let mut simulator = Simulator::new(vec![Vec::new()], StdRng::from_seed(Default::default()));
        for event in 0..100 {
            simulator.schedule_event(Duration::from_secs(1), 0, event);
        }
        while !simulator.run_one() {}
        assert_eq!(simulator.states[0], (0..100).collect::<Vec<_>>());
    }

    #[test]
    pub fn test_simulator() {
        let mut simulator = Simulator::new(vec![0u64, 0u64], StdRng::from_seed(Default::default()));
        simulator.schedule_event(Duration::from_secs(3), 0, 2);
        simulator.schedule_event(Duration::from_secs(1), 1, 4);
        simulator.schedule_event(Duration::from_secs(4), 0, 8);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(1));
        assert_eq!(simulator.states, vec![0, 4]);
        simulator.schedule_event(Duration::from_secs(1), 1, 1);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(2));
        assert_eq!(simulator.states, vec![0, 5]);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(3));
        assert_eq!(simulator.states, vec![2, 5]);

        assert!(simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(4));
        assert_eq!(simulator.states, vec![10, 5]);
    }
}
