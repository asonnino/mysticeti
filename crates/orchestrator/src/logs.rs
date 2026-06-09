// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::cmp::max;

/// Snapshot of log-analysis state — the data callers need to render or react to
/// a benchmark's log outcome. Produced by [`LogsAnalyzer::summarize`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogsReport {
    pub node_panic: bool,
    pub client_panic: bool,
    pub node_errors: usize,
    pub client_errors: usize,
}

/// A simple log analyzer counting the number of errors and panics.
#[derive(Default, PartialEq, Eq)]
pub(crate) struct LogsAnalyzer {
    /// The number of errors in the nodes' log files.
    node_errors: usize,
    /// Whether a node panicked.
    node_panic: bool,
    /// The number of errors in the clients' log files.
    client_errors: usize,
    /// Whether a client panicked.
    client_panic: bool,
}

impl LogsAnalyzer {
    /// Deduce the number of nodes errors from the logs.
    pub(crate) fn set_node_errors(&mut self, log: &str) {
        self.node_errors = log.matches(" ERROR").count();
        self.node_panic = log.contains("panic");
    }

    /// Deduce the number of clients errors from the logs.
    pub(crate) fn set_client_errors(&mut self, log: &str) {
        self.client_errors = max(self.client_errors, log.matches(" ERROR").count());
        self.client_panic = log.contains("panic");
    }

    /// Snapshot the analyzer's state into a [`LogsReport`] — the data the
    /// caller renders into a banner or reacts to programmatically.
    pub(crate) fn summarize(&self) -> LogsReport {
        LogsReport {
            node_panic: self.node_panic,
            client_panic: self.client_panic,
            node_errors: self.node_errors,
            client_errors: self.client_errors,
        }
    }
}
