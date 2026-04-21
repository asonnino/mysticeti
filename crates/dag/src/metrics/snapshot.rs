// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use prometheus::proto::MetricFamily;

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
    pub fn metric(&self, name: &str, label_values: &[(&str, &str)]) -> f64 {
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                if !Self::labels_match(metric, label_values) {
                    continue;
                }
                if metric.has_counter() {
                    return metric.get_counter().get_value();
                } else if metric.has_gauge() {
                    return metric.get_gauge().get_value();
                } else if metric.has_untyped() {
                    return metric.get_untyped().get_value();
                }
            }
        }
        0.0
    }

    /// Mean of all observations in a histogram, in the histogram's
    /// native unit. Returns `None` when the histogram is absent or
    /// has zero observations.
    pub fn histogram_mean(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let (sum, count) = self.histogram_stats(name, labels)?;
        (count > 0).then(|| sum / count as f64)
    }

    /// Mean end-to-end transaction latency (ms) observed by this
    /// replica. Returns `None` when no committed transaction reached
    /// `update_metrics`.
    pub fn mean_latency_ms(&self) -> Option<f64> {
        self.histogram_mean("latency_s", &[("workload", "shared")])
            .map(|seconds| seconds * 1000.0)
    }

    /// Number of blocks this replica knows it's still missing from
    /// the given peer authority.
    pub fn missing_blocks(&self, authority: &str) -> i64 {
        self.metric("missing_blocks", &[("authority", authority)]) as i64
    }

    /// Total committed leaders for `authority`, summed across all
    /// `commit_type` labels (direct-commit, indirect-skip, ...).
    pub fn committed_leaders(&self, authority: &str) -> u64 {
        let mut total = 0.0;
        for family in &self.families {
            if family.get_name() != "committed_leaders_total" {
                continue;
            }
            for metric in family.get_metric() {
                let matches = metric
                    .get_label()
                    .iter()
                    .any(|l| l.get_name() == "authority" && l.get_value() == authority);
                if matches && metric.has_counter() {
                    total += metric.get_counter().get_value();
                }
            }
        }
        total as u64
    }

    /// Committed leaders per second observed by this replica.
    /// Returns `None` when `duration` is zero or nothing committed.
    pub fn leader_commits_per_second(&self, authority: &str, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let count = self.committed_leaders(authority);
        (count > 0).then(|| count as f64 / duration.as_secs_f64())
    }

    /// Read a histogram's sample sum and count. Returns `None` when no
    /// matching histogram is found (distinct from a present histogram
    /// with zero observations, which returns `Some((0.0, 0))`).
    pub fn histogram_stats(&self, name: &str, labels: &[(&str, &str)]) -> Option<(f64, u64)> {
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                if !metric.has_histogram() || !Self::labels_match(metric, labels) {
                    continue;
                }
                let histogram = metric.get_histogram();
                return Some((histogram.get_sample_sum(), histogram.get_sample_count()));
            }
        }
        None
    }

    fn labels_match(metric: &prometheus::proto::Metric, expected: &[(&str, &str)]) -> bool {
        let actual = metric.get_label();
        actual.len() == expected.len()
            && expected.iter().all(|(key, value)| {
                actual
                    .iter()
                    .any(|l| l.get_name() == *key && l.get_value() == *value)
            })
    }
}

#[cfg(test)]
mod test {
    use super::MetricsSnapshot;
    use prometheus::{
        Registry, register_int_counter_vec_with_registry, register_int_counter_with_registry,
        register_int_gauge_with_registry,
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
}
