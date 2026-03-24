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
