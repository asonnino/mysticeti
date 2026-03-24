// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use prometheus::{proto::MetricFamily, TextEncoder};

/// A point-in-time snapshot of all metrics from a Prometheus
/// registry. Test-only — no production cost.
#[derive(Debug)]
pub struct MetricsSnapshot {
    families: Vec<MetricFamily>,
}

impl MetricsSnapshot {
    pub(super) fn from_families(families: Vec<MetricFamily>) -> Self {
        Self { families }
    }

    /// Read a metric by name and optional label key-value pairs.
    pub fn metric(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.find(name, labels)
    }

    fn find(&self, name: &str, label_values: &[(&str, &str)]) -> f64 {
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                let labels = metric.get_label();
                let matches = label_values.iter().all(|(key, value)| {
                    labels
                        .iter()
                        .any(|l| l.get_name() == *key && l.get_value() == *value)
                }) && labels.len() == label_values.len();

                if matches {
                    if metric.has_counter() {
                        return metric.get_counter().get_value();
                    } else if metric.has_gauge() {
                        return metric.get_gauge().get_value();
                    } else if metric.has_untyped() {
                        return metric.get_untyped().get_value();
                    }
                }
            }
        }
        0.0
    }
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoder = TextEncoder::new();
        let text = encoder
            .encode_to_string(&self.families)
            .map_err(|_| fmt::Error)?;
        f.write_str(&text)
    }
}
