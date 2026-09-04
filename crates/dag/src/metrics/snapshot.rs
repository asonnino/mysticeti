// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use prometheus::{Encoder, TextEncoder, proto::MetricFamily};

use super::names::{
    COMMIT_TYPE_DIRECT_COMMIT, COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_INDIRECT_COMMIT,
    COMMIT_TYPE_INDIRECT_SKIP, COMMITTED_LEADERS_TOTAL, LABEL_COMMIT_TYPE, LATENCY_S,
    LEADER_TIMEOUT_TOTAL, STEELHEAD_PERIOD,
};

/// Cumulative `(upper_bound, count)` buckets plus a histogram's sample sum
/// and count.
type HistogramBuckets = (Vec<(f64, u64)>, f64, u64);

/// A point-in-time snapshot of all metrics from a Prometheus
/// registry. Test-only — no production cost.
#[derive(Debug)]
pub struct MetricsSnapshot {
    families: Vec<MetricFamily>,
}

impl MetricsSnapshot {
    pub(super) fn new(families: Vec<MetricFamily>) -> Self {
        Self { families }
    }

    /// Number of leader timeouts observed by this replica.
    pub fn leader_timeouts(&self) -> u64 {
        self.scalar_value(LEADER_TIMEOUT_TOTAL, &[]) as u64
    }

    /// Percentile `p` (in `0.0..=1.0`) of this replica's committed-transaction latency
    /// histogram, in milliseconds. Returns `None` when the histogram is absent or empty.
    pub fn latency_percentile_ms(&self, p: f64) -> Option<f64> {
        self.histogram_percentile(LATENCY_S, p)
            .map(|seconds| seconds * 1000.0)
    }

    /// Windowed latency percentile in milliseconds: percentile `p` over the
    /// observations recorded between `previous` and this snapshot. `None` when
    /// the window is empty or the histograms are absent or inconsistent.
    pub fn latency_window_percentile_ms(&self, previous: &Self, p: f64) -> Option<f64> {
        self.histogram_window_percentile(previous, LATENCY_S, p)
            .map(|seconds| seconds * 1000.0)
    }

    /// Windowed mean latency in milliseconds over the observations recorded
    /// between `previous` and this snapshot; `None` on an empty window.
    pub fn latency_window_mean_ms(&self, previous: &Self) -> Option<f64> {
        self.histogram_window_mean(previous, LATENCY_S)
            .map(|seconds| seconds * 1000.0)
    }

    /// Mean committed-transaction latency in milliseconds since startup;
    /// `None` when the histogram is absent or empty.
    pub fn latency_mean_ms(&self) -> Option<f64> {
        let (sum, count) = self.histogram_sum_and_count(LATENCY_S)?;
        (count > 0).then(|| sum / count as f64 * 1000.0)
    }

    /// Total committed transactions observed by this replica, taken from the
    /// `latency_s` histogram sample-count (one observation per committed
    /// transaction). `0` when the histogram is absent or empty.
    pub fn total_committed_transactions(&self) -> u64 {
        self.histogram_sum_and_count(LATENCY_S)
            .map(|(_, count)| count)
            .unwrap_or(0)
    }

    /// This replica's committed-transaction rate (TPS) over `duration`.
    /// `None` when `duration` is zero.
    pub fn transactions_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        Some(self.total_committed_transactions() as f64 / duration.as_secs_f64())
    }

    /// Total committed leaders observed by this replica. Skipped leaders are excluded.
    pub fn total_committed_leaders(&self) -> u64 {
        let Some(family) = self.find_family(COMMITTED_LEADERS_TOTAL) else {
            return 0;
        };
        let mut total = 0.0;
        for metric in family.get_metric() {
            let is_commit = metric.get_label().iter().any(|l| {
                l.name() == LABEL_COMMIT_TYPE
                    && matches!(
                        l.value(),
                        COMMIT_TYPE_DIRECT_COMMIT | COMMIT_TYPE_INDIRECT_COMMIT,
                    )
            });
            if is_commit && metric.counter.is_some() {
                total += metric.counter.value();
            }
        }
        total as u64
    }

    /// This replica's committed-leader rate (leaders/s) over `duration`.
    /// `None` when `duration` is zero.
    pub fn committed_leaders_per_second(&self, duration: Duration) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        Some(self.total_committed_leaders() as f64 / duration.as_secs_f64())
    }

    /// Leaders committed by the direct rule (fast or slow path).
    pub fn direct_commits(&self) -> u64 {
        self.commit_type_total(COMMIT_TYPE_DIRECT_COMMIT)
    }

    /// Leaders committed by the indirect rule (via an anchor).
    pub fn indirect_commits(&self) -> u64 {
        self.commit_type_total(COMMIT_TYPE_INDIRECT_COMMIT)
    }

    /// Leaders skipped by the direct rule (a quorum of blames).
    pub fn direct_skips(&self) -> u64 {
        self.commit_type_total(COMMIT_TYPE_DIRECT_SKIP)
    }

    /// The Steelhead period in force (0 = infinite, i.e. pure sync rule).
    pub fn steelhead_period(&self) -> u64 {
        let Some(family) = self.find_family(STEELHEAD_PERIOD) else {
            return 0;
        };
        family
            .get_metric()
            .first()
            .and_then(|metric| metric.gauge.as_ref())
            .map(|gauge| gauge.value() as u64)
            .unwrap_or(0)
    }

    /// Leaders skipped by the indirect rule (via an anchor).
    pub fn indirect_skips(&self) -> u64 {
        self.commit_type_total(COMMIT_TYPE_INDIRECT_SKIP)
    }

    /// Decided leaders of one `commit_type`, summed across leader authorities.
    fn commit_type_total(&self, commit_type: &str) -> u64 {
        let Some(family) = self.find_family(COMMITTED_LEADERS_TOTAL) else {
            return 0;
        };
        let mut total = 0.0;
        for metric in family.get_metric() {
            let type_matches = metric
                .get_label()
                .iter()
                .any(|label| label.name() == LABEL_COMMIT_TYPE && label.value() == commit_type);
            if type_matches && metric.counter.is_some() {
                total += metric.counter.value();
            }
        }
        total as u64
    }

    /// Render the snapshot in the Prometheus text exposition format — the same format every
    /// Prometheus scrape endpoint emits, parseable by `promtool`, Prometheus itself, and most
    /// TSDB ingesters.
    pub fn to_prometheus_text(&self) -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&self.families, &mut buffer)
            .expect("TextEncoder writing to Vec cannot fail");
        String::from_utf8(buffer).expect("prometheus text format is UTF-8")
    }

    /// Read a single scalar value (counter / gauge / untyped) by name + exact label match.
    /// Internal building block for domain-named accessors (`leader_timeouts`, …) and for
    /// tests; the public API intentionally stays domain-named rather than exposing a generic
    /// probe.
    pub(super) fn scalar_value(&self, name: &str, label_values: &[(&str, &str)]) -> f64 {
        let Some(family) = self.find_family(name) else {
            return 0.0;
        };
        for metric in family.get_metric() {
            let actual = metric.get_label();
            let labels_match = actual.len() == label_values.len()
                && label_values.iter().all(|(key, value)| {
                    actual
                        .iter()
                        .any(|l| l.name() == *key && l.value() == *value)
                });
            if !labels_match {
                continue;
            }
            if metric.counter.is_some() {
                return metric.counter.value();
            } else if metric.gauge.is_some() {
                return metric.gauge.value();
            } else if metric.untyped.is_some() {
                return metric.untyped.value();
            }
        }
        0.0
    }

    /// Percentile `p` (clamped to `[0, 1]`) of a histogram's observations, in the histogram's
    /// native unit. Uses the Prometheus `histogram_quantile` idiom: linear interpolation between
    /// the upper bounds of adjacent buckets. Returns `None` when the histogram is absent or has
    /// zero observations. When the selected bucket is the `+Inf` terminal, falls back to the
    /// previous finite upper bound so the result stays plottable.
    pub(super) fn histogram_percentile(&self, name: &str, p: f64) -> Option<f64> {
        let (buckets, _, total) = self.histogram_buckets(name)?;
        percentile_from_buckets(name, &buckets, total, p)
    }

    /// Percentile `p` over the observations recorded between `previous` and
    /// this snapshot, computed from the bucket-count deltas. `None` when the
    /// window is empty or the two histograms are absent or inconsistent
    /// (mismatched bounds, decreasing counts).
    pub(super) fn histogram_window_percentile(
        &self,
        previous: &Self,
        name: &str,
        p: f64,
    ) -> Option<f64> {
        let (current, _, current_total) = self.histogram_buckets(name)?;
        let (earlier, _, earlier_total) = previous.histogram_buckets(name)?;
        let total = current_total.checked_sub(earlier_total)?;
        if total == 0 || current.len() != earlier.len() {
            return None;
        }
        let mut window = Vec::with_capacity(current.len());
        for (&(upper, count), &(earlier_upper, earlier_count)) in current.iter().zip(&earlier) {
            let same_bound = upper == earlier_upper || (upper.is_nan() && earlier_upper.is_nan());
            if !same_bound {
                return None;
            }
            window.push((upper, count.checked_sub(earlier_count)?));
        }
        percentile_from_buckets(name, &window, total, p)
    }

    /// Mean of the observations recorded between `previous` and this snapshot,
    /// in the histogram's native unit; `None` on an empty window.
    pub(super) fn histogram_window_mean(&self, previous: &Self, name: &str) -> Option<f64> {
        let (sum, count) = self.histogram_sum_and_count(name)?;
        let (earlier_sum, earlier_count) = previous.histogram_sum_and_count(name)?;
        let window_count = count.checked_sub(earlier_count)?;
        if window_count == 0 {
            return None;
        }
        Some((sum - earlier_sum) / window_count as f64)
    }

    /// A histogram's cumulative `(upper_bound, count)` buckets plus its sample
    /// sum and count; `None` when the metric is absent or has no buckets.
    fn histogram_buckets(&self, name: &str) -> Option<HistogramBuckets> {
        let family = self.find_family(name)?;
        for metric in family.get_metric() {
            if metric.histogram.is_none() {
                continue;
            }
            let histogram = metric.get_histogram();
            let buckets: Vec<_> = histogram
                .get_bucket()
                .iter()
                .map(|bucket| (bucket.upper_bound(), bucket.cumulative_count()))
                .collect();
            if buckets.is_empty() {
                return None;
            }
            return Some((
                buckets,
                histogram.get_sample_sum(),
                histogram.get_sample_count(),
            ));
        }
        None
    }

    /// Read a histogram's sample sum and count. Returns `None` when no
    /// matching histogram is found (distinct from a present histogram
    /// with zero observations, which returns `Some((0.0, 0))`).
    pub fn histogram_sum_and_count(&self, name: &str) -> Option<(f64, u64)> {
        let family = self.find_family(name)?;
        for metric in family.get_metric() {
            if metric.histogram.is_none() {
                continue;
            }
            let histogram = metric.get_histogram();
            return Some((histogram.get_sample_sum(), histogram.get_sample_count()));
        }
        None
    }

    fn find_family(&self, name: &str) -> Option<&MetricFamily> {
        self.families.iter().find(|f| f.name() == name)
    }
}

/// Percentile `p` (clamped to `[0, 1]`) over cumulative `(upper_bound, count)`
/// buckets totaling `total` observations, in the histogram's native unit. Uses
/// the Prometheus `histogram_quantile` idiom: linear interpolation between the
/// upper bounds of adjacent buckets. When the selected bucket is the `+Inf`
/// terminal, falls back to the previous finite upper bound so the result stays
/// plottable.
fn percentile_from_buckets(name: &str, buckets: &[(f64, u64)], total: u64, p: f64) -> Option<f64> {
    if total == 0 || buckets.is_empty() {
        return None;
    }
    let target = p.clamp(0.0, 1.0) * total as f64;
    let mut prev_bound = 0.0_f64;
    let mut prev_count = 0_u64;
    let mut last_finite_bound = 0.0_f64;
    for &(upper, count) in buckets {
        if count as f64 >= target {
            let high = if upper.is_finite() {
                upper
            } else {
                last_finite_bound
            };
            if count == prev_count {
                return Some(prev_bound);
            }
            let fraction = (target - prev_count as f64) / (count - prev_count) as f64;
            return Some(prev_bound + fraction * (high - prev_bound));
        }
        if upper.is_finite() {
            last_finite_bound = upper;
        }
        prev_bound = if upper.is_finite() { upper } else { prev_bound };
        prev_count = count;
    }
    // The `+Inf` bucket's cumulative_count should always equal total, so the
    // `count >= target` branch must fire before falling out of the loop. If we
    // still get here the metric data is malformed — log and treat as unavailable
    // rather than panic in the reporting path.
    tracing::error!("malformed histogram {name:?}: cumulative_count never reaches sample_count");
    None
}

#[cfg(test)]
mod test {
    use super::MetricsSnapshot;
    use crate::authority::Authority;
    use crate::metrics::names::{
        COMMIT_TYPE_DIRECT_COMMIT, COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_INDIRECT_COMMIT,
        COMMIT_TYPE_INDIRECT_SKIP,
    };
    use prometheus::{
        Registry, register_histogram_with_registry, register_int_counter_vec_with_registry,
        register_int_counter_with_registry, register_int_gauge_with_registry,
    };

    fn collect_snapshot(registry: &Registry) -> MetricsSnapshot {
        MetricsSnapshot::new(registry.gather())
    }

    #[test]
    fn counter_lookup() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("test_counter", "help", registry).unwrap();
        counter.inc_by(5);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.scalar_value("test_counter", &[]), 5.0);
    }

    #[test]
    fn gauge_lookup() {
        let registry = Registry::new();
        let gauge = register_int_gauge_with_registry!("test_gauge", "help", registry).unwrap();
        gauge.set(42);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.scalar_value("test_gauge", &[]), 42.0);
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
        assert_eq!(
            snapshot.scalar_value("request_total", &[("method", "GET")]),
            3.0
        );
        assert_eq!(
            snapshot.scalar_value("request_total", &[("method", "POST")]),
            7.0
        );
        assert_eq!(
            snapshot.scalar_value("request_total", &[("method", "PUT")]),
            0.0
        );
    }

    #[test]
    fn not_found_returns_zero() {
        let registry = Registry::new();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.scalar_value("nonexistent", &[]), 0.0);
    }

    #[test]
    fn committed_leaders_excludes_skips() {
        // Drives `committed_leaders_total` directly so the test doesn't need a `Data<Block>` to
        // construct `LeaderStatus::DirectCommit`. Label values here must match the wire strings
        // that `Metrics::inc_decided_leaders` writes.
        let authority = Authority::from(0_usize).to_string();
        let registry = Registry::new();
        let counter = register_int_counter_vec_with_registry!(
            "committed_leaders_total",
            "help",
            &["authority", "commit_type"],
            registry
        )
        .unwrap();
        counter
            .with_label_values(&[authority.as_str(), COMMIT_TYPE_DIRECT_COMMIT])
            .inc();
        counter
            .with_label_values(&[authority.as_str(), COMMIT_TYPE_INDIRECT_COMMIT])
            .inc();
        counter
            .with_label_values(&[authority.as_str(), COMMIT_TYPE_DIRECT_SKIP])
            .inc();
        counter
            .with_label_values(&[authority.as_str(), COMMIT_TYPE_INDIRECT_SKIP])
            .inc();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.total_committed_leaders(), 2);
    }

    #[test]
    fn histogram_percentile_interpolates_within_bucket() {
        // Buckets ≤0.25, ≤0.5, ≤0.75, ≤1.0, ≤+Inf. 100 obs uniformly spread across each of the
        // first four buckets gives cumulative counts [100, 200, 300, 400, 400]; p50 sits at
        // target=200, exactly the upper edge of the second bucket, so it should return 0.5.
        let registry = Registry::new();
        let histogram = register_histogram_with_registry!(
            "demo_latency_s",
            "help",
            vec![0.25, 0.5, 0.75, 1.0],
            registry
        )
        .unwrap();
        for value in [0.1, 0.3, 0.6, 0.8] {
            for _ in 0..100 {
                histogram.observe(value);
            }
        }
        let snapshot = collect_snapshot(&registry);
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", 0.0),
            Some(0.0)
        );
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", 0.5),
            Some(0.5)
        );
        // p90 target = 360, lies in the fourth bucket between cumulative 300 and 400 → 0.75 +
        // 0.6 * 0.25 = 0.9.
        let p90 = snapshot
            .histogram_percentile("demo_latency_s", 0.9)
            .unwrap();
        assert!((p90 - 0.9).abs() < 1e-9, "p90 = {p90}");
        // p100: prometheus crate adds an implicit +Inf bucket; fall back to the previous finite
        // edge.
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", 1.0),
            Some(1.0)
        );
    }

    #[test]
    fn histogram_percentile_empty_returns_none() {
        let registry = Registry::new();
        let _histogram =
            register_histogram_with_registry!("demo_empty_s", "help", vec![0.25, 0.5], registry)
                .unwrap();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.histogram_percentile("demo_empty_s", 0.5), None);
    }

    #[test]
    fn histogram_window_percentile_uses_only_the_window() {
        // 100 observations land in the first bucket before the earlier snapshot;
        // 100 more land in the third bucket afterwards. The windowed p50 must
        // interpolate inside the third bucket (0.5..0.75 -> 0.625), while the
        // cumulative p50 over all 200 sits at the first bucket edge.
        let registry = Registry::new();
        let histogram = register_histogram_with_registry!(
            "demo_window_s",
            "help",
            vec![0.25, 0.5, 0.75, 1.0],
            registry
        )
        .unwrap();
        for _ in 0..100 {
            histogram.observe(0.1);
        }
        let earlier = collect_snapshot(&registry);
        for _ in 0..100 {
            histogram.observe(0.6);
        }
        let snapshot = collect_snapshot(&registry);
        assert_eq!(
            snapshot.histogram_window_percentile(&earlier, "demo_window_s", 0.5),
            Some(0.625)
        );
        assert_eq!(
            snapshot.histogram_percentile("demo_window_s", 0.5),
            Some(0.25)
        );
    }

    #[test]
    fn histogram_window_empty_returns_none() {
        let registry = Registry::new();
        let histogram =
            register_histogram_with_registry!("demo_idle_s", "help", vec![0.25, 0.5], registry)
                .unwrap();
        histogram.observe(0.1);
        let earlier = collect_snapshot(&registry);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(
            snapshot.histogram_window_percentile(&earlier, "demo_idle_s", 0.5),
            None
        );
        assert_eq!(
            snapshot.histogram_window_mean(&earlier, "demo_idle_s"),
            None
        );
    }

    #[test]
    fn latency_window_wrappers_convert_to_ms() {
        let registry = Registry::new();
        let histogram =
            register_histogram_with_registry!("latency_s", "help", vec![0.25, 0.5, 1.0], registry)
                .unwrap();
        histogram.observe(0.1);
        let earlier = collect_snapshot(&registry);
        for _ in 0..10 {
            histogram.observe(0.6);
        }
        let snapshot = collect_snapshot(&registry);
        let mean = snapshot.latency_window_mean_ms(&earlier).unwrap();
        assert!((mean - 600.0).abs() < 1e-9, "window mean {mean}");
        let p50 = snapshot
            .latency_window_percentile_ms(&earlier, 0.5)
            .unwrap();
        assert!((500.0..=1000.0).contains(&p50), "window p50 {p50}");
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
        assert_eq!(snapshot.scalar_value("label_test", &[("a", "x")]), 0.0);
        // No labels → mismatch
        assert_eq!(snapshot.scalar_value("label_test", &[]), 0.0);
        // Correct labels → match
        assert_eq!(
            snapshot.scalar_value("label_test", &[("a", "x"), ("b", "y")]),
            1.0
        );
    }
}
