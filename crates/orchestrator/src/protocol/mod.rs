// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    path::{Path, PathBuf},
};

use eyre::Context;
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

use crate::{benchmark::BenchmarkParameters, client::Instance, collector::MetricSpec};

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
    fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> impl Future<Output = String> + Send
    where
        I: Iterator<Item = &'a Instance> + Send;

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

/// The metrics exposed by the protocol that the orchestrator's collector
/// should query from Prometheus.
pub trait ProtocolMetrics {
    /// Describe each metric and what kind of instrumentation backs it. The
    /// collector synthesises the right PromQL per `MetricKind`: `rate()` for
    /// counters, the raw name for gauges, and `histogram_quantile(rate(_bucket))`
    /// for histograms.
    fn metrics(&self) -> Vec<MetricSpec>;

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
