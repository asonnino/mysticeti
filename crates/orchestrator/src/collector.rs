// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PromQL-based metrics collector. Consumers construct a [`Collector`] with the
//! Prometheus address handed back by [`crate::orchestrator::Orchestrator::start_monitoring`]
//! and a list of metric names to query, then drive [`Collector::collect`] on
//! whatever cadence they choose. The collector accumulates every returned
//! sample (with its full Prometheus label map) into a [`BenchmarkResults`]
//! struct that [`Collector::save`] serialises to JSON.
//!
//! Modelled on tidehunter's `collector.rs`, with one deviation: tidehunter
//! discards samples without an `le` label (its instrumentation is histogram-
//! only). The mysticeti instrumentation mixes histograms with counters and
//! gauges, so this collector retains every sample and lets the caller filter
//! by labels post-hoc.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use futures::future::try_join_all;
use prometheus_http_query::{Client as PrometheusClient, response::Data};
use serde::{Deserialize, Serialize};
use serde_yaml;

use crate::{
    benchmark::BenchmarkParameters,
    error::{MonitorError, TestbedResult},
    protocol::ProtocolParameters,
};

/// PromQL `rate()` window applied to [`MetricKind::Counter`] and histogram
/// buckets. Sized at 4× the default [`crate::settings::Settings::scrape_interval`]
/// (15s) so the rate is stable across a few scrapes.
const RATE_WINDOW: &str = "1m";

/// Quantiles synthesised for each [`MetricKind::Histogram`]. Each quantile
/// fires one `histogram_quantile(q, rate(name_bucket[RATE_WINDOW]))` query and
/// stores the results under `{name}.p{quantile*100}`.
const HISTOGRAM_QUANTILES: &[f64] = &[0.5, 0.9, 0.99];

/// What kind of Prometheus instrumentation backs a metric. The collector uses
/// this to synthesise the right PromQL expression — consumers describe what
/// the metric *is*, not how to query it.
pub enum MetricKind {
    /// Cumulative monotonic counter. Synthesised as `rate(name[window])`, so
    /// samples are per-second rates rather than cumulative values.
    Counter,
    /// Instantaneous value. Queried as the raw metric name. Used for metrics
    /// where the cumulative-rate interpretation would be wrong — e.g. an
    /// elapsed-seconds counter exposed as `benchmark_duration` where
    /// `rate(benchmark_duration[1m]) ≈ 1.0` always.
    Gauge,
    /// Histogram. Synthesised as `histogram_quantile(q, rate(name_bucket[window]))`
    /// for each `q` in [`HISTOGRAM_QUANTILES`]; samples land under
    /// `{name}.p50` / `{name}.p90` / `{name}.p99`.
    Histogram,
}

/// Consumer-facing metric description: what to query and where to store the
/// result. Replaces the bridging `metric_names()` of #98.
pub struct MetricSpec {
    pub name: String,
    pub kind: MetricKind,
}

/// One Prometheus sample: an instant query returns a vector of these, one per
/// matching time series. `labels` is the full label map for that series; the
/// consumer filters or groups by it (e.g. `labels["workload"] == "owned"`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Sample {
    timestamp: f64,
    value: f64,
    labels: HashMap<String, String>,
}

/// Accumulated benchmark output. Keyed by metric name, each entry holds every
/// sample observed across every [`Collector::collect`] tick.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkResults<N, C> {
    parameters: BenchmarkParameters<N, C>,
    samples: HashMap<String, Vec<Sample>>,
}

impl<N: Serialize, C: Serialize> BenchmarkResults<N, C> {
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).expect("BenchmarkResults always serialises to YAML")
    }
}

/// Issues PromQL queries against a deployed Prometheus instance and
/// accumulates the resulting samples in a [`BenchmarkResults`] that the caller
/// periodically persists with [`Collector::save`]. The specific PromQL fired
/// for each metric is derived from its [`MetricKind`].
pub struct Collector<N, C> {
    client: PrometheusClient,
    metrics: Vec<MetricSpec>,
    results: BenchmarkResults<N, C>,
}

impl<N: ProtocolParameters, C: ProtocolParameters> Collector<N, C> {
    /// Return a reference to the accumulated results.
    pub fn results(&self) -> &BenchmarkResults<N, C> {
        &self.results
    }

    /// `prometheus_address` is the URL the consumer received in
    /// [`crate::orchestrator::MonitoringReport::prometheus_address`].
    pub fn new(
        prometheus_address: &str,
        mut parameters: BenchmarkParameters<N, C>,
        metrics: Vec<MetricSpec>,
    ) -> TestbedResult<Self> {
        // The parameters end up serialised to disk via `save()`. Strip any
        // access token from the repository URL before storing so credentials
        // never leak into the results file.
        parameters.settings.repository.remove_access_token();
        let client: PrometheusClient = prometheus_address
            .try_into()
            .map_err(MonitorError::PrometheusError)?;
        Ok(Self {
            client,
            metrics,
            results: BenchmarkResults {
                parameters,
                samples: HashMap::new(),
            },
        })
    }

    /// Synthesise PromQL per [`MetricKind`] and fire every query in parallel.
    /// Histograms fan out to one query per [`HISTOGRAM_QUANTILES`] entry; the
    /// returned samples land under `{name}.p{quantile*100}`. Everything else
    /// stores under the spec's `name`.
    pub async fn collect(&mut self) -> Result<(), MonitorError> {
        // (storage_key, promql) pairs for this tick.
        let mut targets: Vec<(String, String)> = Vec::new();
        for spec in &self.metrics {
            match spec.kind {
                MetricKind::Counter => targets.push((
                    spec.name.clone(),
                    format!("rate({}[{}])", spec.name, RATE_WINDOW),
                )),
                MetricKind::Gauge => targets.push((spec.name.clone(), spec.name.clone())),
                MetricKind::Histogram => {
                    for quantile in HISTOGRAM_QUANTILES {
                        let key = format!("{}.p{}", spec.name, (quantile * 100.0).round() as u32);
                        let promql = format!(
                            "histogram_quantile({}, rate({}_bucket[{}]))",
                            quantile, spec.name, RATE_WINDOW
                        );
                        targets.push((key, promql));
                    }
                }
            }
        }

        let responses = try_join_all(
            targets
                .iter()
                .map(|(_, promql)| self.client.query(promql).get()),
        )
        .await?;

        for ((key, _), response) in targets.iter().zip(&responses) {
            let Data::Vector(vector) = response.data() else {
                // Instant queries always return Vector; anything else is a
                // Prometheus configuration error worth surfacing.
                return Err(MonitorError::UnexpectedPrometheusResponse);
            };
            let entry = self.results.samples.entry(key.clone()).or_default();
            for element in vector {
                entry.push(Sample {
                    timestamp: element.sample().timestamp(),
                    value: element.sample().value(),
                    labels: element.metric().clone(),
                });
            }
        }

        Ok(())
    }

    /// Write the accumulated results to `dir/measurements-{parameters:?}.yaml`.
    pub fn save(&self, dir: &Path) -> Result<(), MonitorError> {
        let yaml = self.results.to_yaml();
        let mut path = PathBuf::from(dir);
        path.push(format!("measurements-{:?}.yaml", self.results.parameters));
        fs::write(&path, yaml)?;
        Ok(())
    }
}
