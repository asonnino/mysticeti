// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

/// Long-lived indeterminate progress spinner. Construct once per [`super::Terminal`],
/// then [`Self::start`] / [`Self::stop`] around regions where the run is "still
/// working" with no other output. On a non-colour terminal the bar is
/// [`ProgressBar::hidden`] so every method is a silent no-op.
pub struct Spinner {
    bar: ProgressBar,
}

impl Spinner {
    pub fn new(color: bool) -> Self {
        let bar = if color {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template(" {spinner} Running… {elapsed}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            bar
        } else {
            ProgressBar::hidden()
        };
        Self { bar }
    }

    /// Begin spinning (or reset the elapsed timer if already running).
    pub fn start(&self) {
        self.bar.reset();
        self.bar.enable_steady_tick(Duration::from_millis(100));
    }

    /// Stop spinning and clear the line. Idempotent.
    pub fn stop(&self) {
        self.bar.disable_steady_tick();
        self.bar.finish_and_clear();
    }

    /// Run `f` while the spinner stays alive: indicatif clears the bar, runs the
    /// closure, then redraws — heartbeat output never fights the spinner for the
    /// line, and the elapsed timer keeps ticking monotonically.
    pub fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        self.bar.suspend(f)
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.bar.disable_steady_tick();
        self.bar.finish_and_clear();
    }
}
