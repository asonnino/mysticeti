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

use crate::{
    benchmark::BenchmarkParameters,
    error::{MonitorError, MonitorResult, TestbedResult},
};

/// One Prometheus sample: an instant query returns a vector of these, one per
/// matching time series. `labels` is the full label map for that series; the
/// consumer filters or groups by it (e.g. `labels["workload"] == "owned"`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub timestamp: f64,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Accumulated benchmark output. Keyed by metric name, each entry holds every
/// sample observed across every [`Collector::collect`] tick.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub parameters: BenchmarkParameters,
    pub samples: HashMap<String, Vec<Sample>>,
}

/// Issues instant PromQL queries against a deployed Prometheus instance and
/// accumulates the resulting samples in a [`BenchmarkResults`] that the caller
/// periodically persists with [`Collector::save`].
pub struct Collector {
    client: PrometheusClient,
    metrics: Vec<String>,
    results: BenchmarkResults,
}

impl Collector {
    /// `prometheus_address` is the URL the consumer received in
    /// [`crate::orchestrator::MonitoringReport::prometheus_address`].
    pub fn new(
        prometheus_address: &str,
        parameters: BenchmarkParameters,
        metrics: Vec<String>,
    ) -> TestbedResult<Self> {
        let client: PrometheusClient = prometheus_address
            .try_into()
            .map_err(|e| MonitorError::PrometheusError(e))?;
        Ok(Self {
            client,
            metrics,
            results: BenchmarkResults {
                parameters,
                samples: HashMap::new(),
            },
        })
    }

    /// Run one instant query per configured metric and append every returned
    /// sample to the accumulator. Queries are fired in parallel; their order
    /// in the response matches the order of `self.metrics`.
    pub async fn collect(&mut self) -> MonitorResult<()> {
        let responses = try_join_all(
            self.metrics
                .iter()
                .map(|metric| self.client.query(metric).get()),
        )
        .await?;

        for (metric_name, response) in self.metrics.iter().zip(&responses) {
            let Data::Vector(vector) = response.data() else {
                // The five mysticeti metrics are all instant-queryable scalars
                // or histogram buckets; anything else is a Prometheus
                // configuration error worth surfacing rather than swallowing.
                return Err(MonitorError::UnexpectedPrometheusResponse);
            };
            let entry = self.results.samples.entry(metric_name.clone()).or_default();
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

    /// Write the accumulated results to `dir/measurements-{parameters:?}.json`.
    /// Preserves today's mysticeti filename convention.
    pub fn save(&self, dir: &Path) -> MonitorResult<()> {
        let json = serde_json::to_string_pretty(&self.results)
            .expect("BenchmarkResults always serialises");
        let mut path = PathBuf::from(dir);
        path.push(format!("measurements-{:?}.json", self.results.parameters));
        fs::write(&path, json)?;
        Ok(())
    }

    pub fn results(&self) -> &BenchmarkResults {
        &self.results
    }
}
