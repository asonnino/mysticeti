// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime};

use tokio::time::{Interval, MissedTickBehavior};
pub use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
    time::{sleep, Instant},
};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TimeInstant(Instant);

#[allow(dead_code)]
impl TimeInstant {
    pub fn now() -> Self {
        Self(Instant::now())
    }

    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

#[allow(dead_code)]
pub fn timestamp_utc() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[allow(dead_code)]
pub struct TimeInterval(Interval);

#[allow(dead_code)]
impl TimeInterval {
    pub fn new(duration: Duration) -> Self {
        let mut interval = tokio::time::interval(duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self(interval)
    }

    pub async fn tick(&mut self) -> TimeInstant {
        TimeInstant(self.0.tick().await)
    }
}
