// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::IntCounter;
use tokio::time::Instant;

/// RAII timer that records elapsed **microseconds** into a
/// Prometheus `IntCounter` on drop. Borrows the counter.
pub struct UtilizationTimer<'a> {
    pub(super) metric: &'a IntCounter,
    pub(super) start: Instant,
}

/// Like [`UtilizationTimer`] but owns the counter. Use when
/// the timer must be moved across `.await` points.
/// Records elapsed **microseconds** on drop.
pub struct OwnedUtilizationTimer {
    pub(super) metric: IntCounter,
    pub(super) start: Instant,
}

impl Drop for UtilizationTimer<'_> {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

impl Drop for OwnedUtilizationTimer {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use prometheus::{Registry, register_int_counter_with_registry};
    use tokio::time::Instant;

    use super::{OwnedUtilizationTimer, UtilizationTimer};

    #[tokio::test(start_paused = true)]
    async fn utilization_timer_records_on_drop() {
        let registry = Registry::new();
        let counter = register_int_counter_with_registry!("utilization", "help", registry).unwrap();
        {
            let _timer = UtilizationTimer {
                metric: &counter,
                start: Instant::now(),
            };
            tokio::time::advance(Duration::from_millis(50)).await;
        }
        assert_eq!(counter.get(), 50_000);
    }

    #[tokio::test(start_paused = true)]
    async fn owned_timer_records_on_drop() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("owned_utilization", "help", registry).unwrap();
        {
            let _timer = OwnedUtilizationTimer {
                metric: counter.clone(),
                start: Instant::now(),
            };
            tokio::time::advance(Duration::from_millis(50)).await;
        }
        assert_eq!(counter.get(), 50_000);
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_drop_records_zero() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("immediate_drop", "help", registry).unwrap();
        {
            let _timer = UtilizationTimer {
                metric: &counter,
                start: Instant::now(),
            };
        }
        assert_eq!(counter.get(), 0);
    }
}
