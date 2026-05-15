// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    path::{Path, PathBuf},
};

use eyre::Context;
use serde::{Serialize, de::DeserializeOwned};

use crate::{benchmark::BenchmarkParameters, client::Instance};

pub mod mysticeti;

pub const BINARY_PATH: &str = "target/release";

pub trait ProtocolParameters:
    Default + Clone + Serialize + DeserializeOwned + Debug + Display
{
    /// Load the configuration from a YAML file located at the provided path.
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, eyre::Error> {
        let path = path.as_ref();
        let error_message = format!("Unable to load config from {}", path.display());
        let reader = std::fs::File::open(path).wrap_err(error_message)?;
        Ok(serde_yaml::from_reader(reader)?)
    }
}

/// The minimum interface that the protocol should implement to allow benchmarks from
/// the orchestrator.
pub trait ProtocolCommands {
    /// The list of dependencies to install (e.g., through apt-get).
    fn protocol_dependencies(&self) -> Vec<&'static str>;

    /// The directories of all databases (that should be erased before each run).
    fn db_directories(&self) -> Vec<PathBuf>;

    /// The command to generate the genesis and all configuration files. This command
    /// is run on each remote machine.
    async fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>;

    /// The command to run a node. The function returns a vector of commands along with the
    /// associated instance on which to run the command.
    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;

    /// The command to run a client. The function returns a vector of commands along with the
    /// associated instance on which to run the command.
    fn client_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;
}

/// The names of the minimum metrics exposed by the protocol that are required to
/// compute performance.
pub trait ProtocolMetrics {
    /// The name of the metric reporting the total duration of the benchmark (in seconds).
    const BENCHMARK_DURATION: &'static str;
    /// The name of the metric reporting the total number of finalized transactions.
    const TOTAL_TRANSACTIONS: &'static str;
    /// The name of the metric reporting the latency buckets.
    const LATENCY_BUCKETS: &'static str;
    /// The name of the metric reporting the sum of the end-to-end latency of all finalized
    /// transactions.
    const LATENCY_SUM: &'static str;
    /// The name of the metric reporting the square of the sum of the end-to-end latency of all
    /// finalized transactions.
    const LATENCY_SQUARED_SUM: &'static str;

    /// The set of metric names the orchestrator should query from Prometheus.
    /// Bridges to the richer `MetricSpec` API planned for #99; for now it's just
    /// the five mandatory constants above.
    fn metric_names(&self) -> Vec<String> {
        vec![
            Self::BENCHMARK_DURATION.into(),
            Self::TOTAL_TRANSACTIONS.into(),
            Self::LATENCY_BUCKETS.into(),
            Self::LATENCY_SUM.into(),
            Self::LATENCY_SQUARED_SUM.into(),
        ]
    }

    /// The network path where the nodes expose prometheus metrics.
    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;

    /// The network path where the clients expose prometheus metrics.
    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;

    /// The command to retrieve the metrics from the nodes.
    fn nodes_metrics_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        self.nodes_metrics_path(instances, parameters)
            .into_iter()
            .map(|(instance, path)| (instance, format!("curl {path}")))
            .collect()
    }

    /// The command to retrieve the metrics from the clients.
    fn clients_metrics_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        self.clients_metrics_path(instances, parameters)
            .into_iter()
            .map(|(instance, path)| (instance, format!("curl {path}")))
            .collect()
    }
}
