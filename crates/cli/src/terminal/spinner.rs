// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

/// Indeterminate progress spinner. Constructor is always safe to call; when stderr isn't
/// a colour terminal the spinner emits nothing and `finish` is a no-op.
pub struct Spinner {
    inner: Option<ProgressBar>,
}

impl Spinner {
    pub fn new(color: bool) -> Self {
        if !color {
            return Self { inner: None };
        }
        println!();
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template(" {spinner} Simulating… {elapsed}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.enable_steady_tick(Duration::from_millis(100));
        Self { inner: Some(bar) }
    }

    pub fn finish(self) {
        if let Some(bar) = self.inner {
            bar.finish_and_clear();
        }
    }
}
