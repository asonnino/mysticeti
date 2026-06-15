// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, mem, time::Duration};

use indicatif::{ProgressBar, ProgressStyle};

use super::{BOLD, GREEN, RED, RESET};

enum Animation {
    Idle,
    Spinner(ProgressBar),
    Bar(ProgressBar),
}

impl Animation {
    fn spinner(label: &str) -> Animation {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template(" {spinner} {msg} ({elapsed})")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(label.to_owned());
        bar.enable_steady_tick(Duration::from_millis(100));
        Animation::Spinner(bar)
    }

    fn bar(total: Duration) -> Animation {
        // `max(1)` keeps a 0s configuration from producing an empty bar.
        let total_secs = total.as_secs().max(1);
        let template = format!("[{BOLD}{{bar:30.bold.blue/blue}}] {{pos}}s / {total_secs}s{RESET}");
        let bar = ProgressBar::new(total_secs);
        bar.set_style(
            ProgressStyle::with_template(&template)
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=>-"),
        );
        bar.enable_steady_tick(Duration::from_millis(100));
        Animation::Bar(bar)
    }
}

/// Live progress indicator for a single run. The mode is fixed for the run via
/// [`Progress::start`]: known total → determinate `Bar`, unknown → indeterminate
/// `Spinner`. On a non-colour terminal every method is a silent no-op.
pub struct Progress {
    color: bool,
    animation: Animation,
}
impl Progress {
    pub fn new(color: bool) -> Self {
        Self {
            color,
            animation: Animation::Idle,
        }
    }

    /// Begin progress display. `total = Some(d)` configures a determinate bar
    /// with known length; `total = None` falls back to an indeterminate spinner
    /// labelled with `label`. Calling `start` again clears the previous
    /// animation and reconfigures it.
    pub fn start(&mut self, total: Option<Duration>, label: &str) {
        self.stop();
        if !self.color {
            return;
        }
        self.animation = match total {
            Some(d) => Animation::bar(d),
            None => Animation::spinner(label),
        };
    }

    /// Stop the indicator and clear the line. Idempotent.
    pub fn stop(&mut self) {
        if let Animation::Spinner(bar) | Animation::Bar(bar) =
            mem::replace(&mut self.animation, Animation::Idle)
        {
            bar.finish_and_clear();
        }
    }

    /// Advance the determinate bar to `elapsed`. No-op for `Spinner` / `Idle`
    /// (indicatif drives the spinner's animation off its own steady tick).
    pub fn set_elapsed(&self, elapsed: Duration) {
        if let Animation::Bar(bar) = &self.animation {
            bar.set_position(elapsed.as_secs());
        }
    }

    /// Run `work` under an indeterminate spinner labelled `label`, then settle
    /// the spinner into a persistent one-line summary and return the result. The natural
    /// home for any "do this one thing while showing a spinner" call site.
    pub async fn track<T, E, F>(&mut self, label: &str, work: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        self.start(None, label);
        let result = work.await;
        self.finish(label, result.is_ok());
        result
    }

    /// Settle the active spinner into a persistent summary line marked with a
    /// green check (success) or red cross (failure), keeping the elapsed time.
    /// On a non-colour terminal the spinner is `Idle`, so this is a no-op.
    fn finish(&mut self, label: &str, success: bool) {
        if let Animation::Spinner(bar) | Animation::Bar(bar) =
            mem::replace(&mut self.animation, Animation::Idle)
        {
            let (color, glyph) = if success {
                (GREEN, "✔")
            } else {
                (RED, "✘")
            };
            bar.set_style(
                ProgressStyle::with_template(" {msg} ({elapsed})")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            bar.finish_with_message(format!("{color}{glyph}{RESET} {label}"));
        }
    }

    /// Run `f` while the bar stays alive: indicatif clears the bar, runs the
    /// closure, then redraws — heartbeat output never fights the bar for the
    /// line, and the elapsed timer keeps ticking monotonically.
    pub fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        match &self.animation {
            Animation::Idle => f(),
            Animation::Spinner(bar) | Animation::Bar(bar) => bar.suspend(f),
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.stop();
    }
}
