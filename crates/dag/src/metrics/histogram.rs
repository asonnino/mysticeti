// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::AddAssign, time::Duration};

use prometheus::{register_int_gauge_vec_with_registry, IntGaugeVec, Registry};
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
