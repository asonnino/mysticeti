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

#[cfg(test)]
mod test {
    use super::MetricsSnapshot;
    use prometheus::{
        register_int_counter_vec_with_registry, register_int_counter_with_registry,
        register_int_gauge_with_registry, Registry,
    };

    fn collect_snapshot(registry: &Registry) -> MetricsSnapshot {
        MetricsSnapshot::from_families(registry.gather())
    }

    #[test]
    fn counter_lookup() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("test_counter", "help", registry).unwrap();
        counter.inc_by(5);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("test_counter", &[]), 5.0);
    }

    #[test]
    fn gauge_lookup() {
        let registry = Registry::new();
        let gauge = register_int_gauge_with_registry!("test_gauge", "help", registry).unwrap();
        gauge.set(42);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("test_gauge", &[]), 42.0);
    }

    #[test]
    fn labeled_metric_lookup() {
        let registry = Registry::new();
        let counter_vec =
            register_int_counter_vec_with_registry!("request_total", "help", &["method"], registry)
                .unwrap();
        counter_vec.with_label_values(&["GET"]).inc_by(3);
        counter_vec.with_label_values(&["POST"]).inc_by(7);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("request_total", &[("method", "GET")]), 3.0);
        assert_eq!(snapshot.metric("request_total", &[("method", "POST")]), 7.0);
        assert_eq!(snapshot.metric("request_total", &[("method", "PUT")]), 0.0);
    }

    #[test]
    fn not_found_returns_zero() {
        let registry = Registry::new();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("nonexistent", &[]), 0.0);
    }

    #[test]
    fn partial_label_mismatch() {
        let registry = Registry::new();
        let counter_vec =
            register_int_counter_vec_with_registry!("label_test", "help", &["a", "b"], registry)
                .unwrap();
        counter_vec.with_label_values(&["x", "y"]).inc();
        let snapshot = collect_snapshot(&registry);
        // Only one label when metric has two → mismatch
        assert_eq!(snapshot.metric("label_test", &[("a", "x")]), 0.0);
        // No labels → mismatch
        assert_eq!(snapshot.metric("label_test", &[]), 0.0);
        // Correct labels → match
        assert_eq!(
            snapshot.metric("label_test", &[("a", "x"), ("b", "y")]),
            1.0
        );
    }

    #[test]
    fn display_is_nonempty() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("display_counter", "help", registry).unwrap();
        counter.inc();
        let snapshot = collect_snapshot(&registry);
        let text = format!("{snapshot}");
        assert!(!text.is_empty());
        assert!(text.contains("display_counter"));
    }
}
