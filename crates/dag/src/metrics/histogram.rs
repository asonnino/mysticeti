// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::AddAssign, time::Duration};

use prometheus::{IntGaugeVec, Registry, register_int_gauge_vec_with_registry};
use tokio::sync::mpsc;

const REPORT_PER_MILLES: [usize; 3] = [500, 900, 990];

/// Compound trait bound for types that can be stored in a
/// precise histogram and published to Prometheus.
pub(super) trait HistogramValue:
    Default + Ord + AddAssign + Copy + AsPrometheusMetric
{
}
impl<T: Default + Ord + AddAssign + Copy + AsPrometheusMetric> HistogramValue for T {}

/// Create an observer/reporter pair for a single metric.
/// Mirrors `mpsc::channel` — observer (sender) first.
/// Create an observer/reporter pair for a single metric.
/// Mirrors `mpsc::channel` — observer (sender) first.
pub(super) fn histogram<T: HistogramValue>(
    registry: &Registry,
    name: &str,
) -> (HistogramObserver<T>, HistogramReporter<T>) {
    let (histogram, observer) = precise_histogram();
    (observer, HistogramReporter::new(registry, name, histogram))
}

/// Create observer/reporter pairs for per-label metrics
/// (e.g. one histogram per peer).
pub(super) fn vec_histogram<T: HistogramValue>(
    registry: &Registry,
    label: &str,
    name: &str,
    peer_labels: impl Iterator<Item = String>,
) -> (Vec<HistogramObserver<T>>, VecHistogramReporter<T>) {
    let mut histograms = Vec::new();
    let mut observers = Vec::new();
    for peer_label in peer_labels {
        let (histogram, observer) = precise_histogram();
        histograms.push((histogram, peer_label));
        observers.push(observer);
    }
    (
        observers,
        VecHistogramReporter::new(registry, label, name, histograms),
    )
}

/// Lock-free observer handle for recording data points.
/// Sends values through an unbounded channel to a
/// [`HistogramReporter`] for deferred percentile computation.
#[derive(Clone)]
pub(super) struct HistogramObserver<T> {
    sender: mpsc::UnboundedSender<T>,
}

impl<T: Send> HistogramObserver<T> {
    pub fn observe(&self, value: T) {
        self.sender.send(value).ok();
    }
}

/// Receiver side of a precise histogram. Collects data points
/// from a [`HistogramObserver`] channel, computes exact
/// percentiles, and publishes them to a Prometheus gauge.
pub(super) struct HistogramReporter<T> {
    histogram: PreciseHistogram<T>,
    gauge: IntGaugeVec,
}

impl<T: HistogramValue> HistogramReporter<T> {
    fn new(registry: &Registry, name: &str, histogram: PreciseHistogram<T>) -> Self {
        let gauge = register_int_gauge_vec_with_registry!(name, name, &["v"], registry).unwrap();
        Self { histogram, gauge }
    }

    pub fn report(&mut self) {
        let Some([p50, p90, p99]) = self.histogram.per_milles(REPORT_PER_MILLES) else {
            return;
        };
        self.gauge
            .with_label_values(&["p50"])
            .set(p50.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p90"])
            .set(p90.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p99"])
            .set(p99.as_prometheus_metric());
        self.gauge
            .with_label_values(&["sum"])
            .set(self.histogram.sum().as_prometheus_metric());
        self.gauge
            .with_label_values(&["count"])
            .set(self.histogram.count().as_prometheus_metric());
    }

    pub fn clear_receive_all(&mut self) {
        self.histogram.clear_receive_all();
    }
}

/// Like [`HistogramReporter`] but for per-label metrics
/// (e.g. one histogram per peer).
pub(super) struct VecHistogramReporter<T> {
    histograms: Vec<(PreciseHistogram<T>, String)>,
    gauge: IntGaugeVec,
}

impl<T: HistogramValue> VecHistogramReporter<T> {
    fn new(
        registry: &Registry,
        label: &str,
        name: &str,
        histograms: Vec<(PreciseHistogram<T>, String)>,
    ) -> Self {
        let gauge =
            register_int_gauge_vec_with_registry!(name, name, &[label, "v"], registry).unwrap();
        Self { histograms, gauge }
    }

    pub fn report(&mut self) {
        for (histogram, label) in self.histograms.iter_mut() {
            let Some([p50, p90, p99]) = histogram.per_milles(REPORT_PER_MILLES) else {
                continue;
            };
            self.gauge
                .with_label_values(&[label, "p50"])
                .set(p50.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "p90"])
                .set(p90.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "p99"])
                .set(p99.as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "sum"])
                .set(histogram.sum().as_prometheus_metric());
            self.gauge
                .with_label_values(&[label, "count"])
                .set(histogram.count().as_prometheus_metric());
        }
    }

    pub fn clear_receive_all(&mut self) {
        self.histograms
            .iter_mut()
            .for_each(|(histogram, _)| histogram.clear_receive_all());
    }
}

pub(super) trait AsPrometheusMetric {
    fn as_prometheus_metric(&self) -> i64;
}

impl AsPrometheusMetric for Duration {
    fn as_prometheus_metric(&self) -> i64 {
        self.as_micros() as i64
    }
}

impl AsPrometheusMetric for usize {
    fn as_prometheus_metric(&self) -> i64 {
        *self as i64
    }
}

fn precise_histogram<T: Default>() -> (PreciseHistogram<T>, HistogramObserver<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let observer = HistogramObserver { sender };
    let histogram = PreciseHistogram {
        points: Vec::new(),
        sum: T::default(),
        count: 0,
        receiver,
    };
    (histogram, observer)
}

struct PreciseHistogram<T> {
    points: Vec<T>,
    sum: T,
    count: usize,
    receiver: mpsc::UnboundedReceiver<T>,
}

impl<T: Ord + AddAssign + Copy + Default> PreciseHistogram<T> {
    fn record(&mut self, point: T) {
        self.points.push(point);
        self.sum += point;
        self.count += 1;
    }

    fn sum(&self) -> T {
        self.sum
    }

    fn count(&self) -> usize {
        self.count
    }

    fn per_milles<const N: usize>(&mut self, per_mille: [usize; N]) -> Option<[T; N]> {
        if self.points.is_empty() {
            return None;
        }
        self.points.sort();
        Some(per_mille.map(|p| self.points[self.per_mille_index(p)]))
    }

    fn per_mille_index(&self, per_mille: usize) -> usize {
        self.points.len() * per_mille.min(999) / 1000
    }

    fn clear_receive_all(&mut self) {
        self.points.clear();
        while let Ok(value) = self.receiver.try_recv() {
            self.record(value);
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use prometheus::Registry;

    use super::{AsPrometheusMetric, histogram, precise_histogram, vec_histogram};

    #[test]
    fn per_milles_basic() {
        let (mut h, _observer) = precise_histogram::<usize>();
        for v in 1..=100 {
            h.record(v);
        }
        let [p50, p90, p99] = h.per_milles([500, 900, 990]).unwrap();
        assert_eq!(p50, 51);
        assert_eq!(p90, 91);
        assert_eq!(p99, 100);
    }

    #[test]
    fn per_milles_single_value() {
        let (mut h, _observer) = precise_histogram::<usize>();
        h.record(42);
        let [p50, p90, p99] = h.per_milles([500, 900, 990]).unwrap();
        assert_eq!(p50, 42);
        assert_eq!(p90, 42);
        assert_eq!(p99, 42);
    }

    #[test]
    fn per_milles_empty() {
        let (mut h, _observer) = precise_histogram::<usize>();
        assert!(h.per_milles([500, 900, 990]).is_none());
    }

    #[test]
    fn per_milles_two_values() {
        let (mut h, _observer) = precise_histogram::<usize>();
        h.record(10);
        h.record(20);
        // index = 2 * p / 1000: p500→1, p900→1, p990→1
        let [p50, p90, p99] = h.per_milles([500, 900, 990]).unwrap();
        assert_eq!(p50, 20);
        assert_eq!(p90, 20);
        assert_eq!(p99, 20);
    }

    #[test]
    fn per_mille_index_clamps() {
        let (mut h, _observer) = precise_histogram::<usize>();
        for v in 0..10 {
            h.record(v);
        }
        // 1000 and above should clamp to 999 → index = 10*999/1000 = 9
        assert_eq!(h.per_mille_index(1000), 9);
        assert_eq!(h.per_mille_index(2000), 9);
        assert_eq!(h.per_mille_index(999), 9);
    }

    #[test]
    fn sum_and_count() {
        let (mut h, _observer) = precise_histogram::<usize>();
        h.record(10);
        h.record(20);
        h.record(30);
        assert_eq!(h.sum(), 60);
        assert_eq!(h.count(), 3);
    }

    #[test]
    fn clear_receive_all_drains_channel() {
        let (mut h, observer) = precise_histogram::<usize>();
        observer.observe(1);
        observer.observe(2);
        observer.observe(3);
        assert!(h.points.is_empty());
        h.clear_receive_all();
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum(), 6);
    }

    #[test]
    fn observer_clone_shares_channel() {
        let (mut h, observer) = precise_histogram::<usize>();
        let observer2 = observer.clone();
        observer.observe(10);
        observer2.observe(20);
        h.clear_receive_all();
        assert_eq!(h.count(), 2);
        assert_eq!(h.sum(), 30);
    }

    #[test]
    fn duration_as_prometheus_metric() {
        let d = Duration::from_micros(1234);
        assert_eq!(d.as_prometheus_metric(), 1234);
    }

    #[test]
    fn usize_as_prometheus_metric() {
        assert_eq!(42usize.as_prometheus_metric(), 42);
    }

    #[test]
    fn report_writes_gauges() {
        let registry = Registry::new();
        let (observer, mut reporter) = histogram::<usize>(&registry, "test_histogram");
        for v in 1..=100 {
            observer.observe(v);
        }
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        assert_eq!(
            snapshot.scalar_value("test_histogram", &[("v", "p50")]),
            51.0
        );
        assert_eq!(
            snapshot.scalar_value("test_histogram", &[("v", "p90")]),
            91.0
        );
        assert_eq!(
            snapshot.scalar_value("test_histogram", &[("v", "p99")]),
            100.0
        );
        assert_eq!(
            snapshot.scalar_value("test_histogram", &[("v", "sum")]),
            5050.0
        );
        assert_eq!(
            snapshot.scalar_value("test_histogram", &[("v", "count")]),
            100.0
        );
    }

    #[test]
    fn report_on_empty_is_noop() {
        let registry = Registry::new();
        let (observer, mut reporter) = histogram::<usize>(&registry, "empty_report_histogram");
        // First window: populate gauges
        for v in [1, 2, 3] {
            observer.observe(v);
        }
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        let p50_after_first = snapshot.scalar_value("empty_report_histogram", &[("v", "p50")]);
        assert!(p50_after_first > 0.0);

        // Second window: no new observations → empty → report early-returns
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        // Gauges retain stale values (not zeroed)
        assert_eq!(
            snapshot.scalar_value("empty_report_histogram", &[("v", "p50")]),
            p50_after_first
        );
    }

    #[test]
    fn clear_receive_all_resets_between_reports() {
        let registry = Registry::new();
        let (observer, mut reporter) = histogram::<usize>(&registry, "windowed_histogram");
        // Window 1
        for v in [10, 20, 30] {
            observer.observe(v);
        }
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        assert_eq!(
            snapshot.scalar_value("windowed_histogram", &[("v", "p50")]),
            20.0
        );

        // Window 2: completely different values
        for v in [100, 200, 300] {
            observer.observe(v);
        }
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        assert_eq!(
            snapshot.scalar_value("windowed_histogram", &[("v", "p50")]),
            200.0
        );
    }

    #[test]
    fn vec_histogram_per_peer() {
        let registry = Registry::new();
        let labels = ["alice", "bob"].iter().map(|s| s.to_string());
        let (observers, mut reporter) =
            vec_histogram::<usize>(&registry, "peer", "per_peer_histogram", labels);
        // Alice gets 1..=10, Bob gets 91..=100
        for v in 1..=10 {
            observers[0].observe(v);
        }
        for v in 91..=100 {
            observers[1].observe(v);
        }
        reporter.clear_receive_all();
        reporter.report();
        let snapshot = crate::metrics::MetricsSnapshot::new(registry.gather());
        // Alice p50: index = 10*500/1000 = 5 → value 6
        assert_eq!(
            snapshot.scalar_value("per_peer_histogram", &[("peer", "alice"), ("v", "p50")]),
            6.0
        );
        // Bob p50: index = 10*500/1000 = 5 → value 96
        assert_eq!(
            snapshot.scalar_value("per_peer_histogram", &[("peer", "bob"), ("v", "p50")]),
            96.0
        );
    }
}
