// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Replica protocol implementation of the orchestrator's protocol traits. Lives
//! in the CLI crate so the orchestrator library stays protocol-agnostic.

use std::{
    fmt::{self, Debug},
    net::IpAddr,
    ops::Deref,
    path::PathBuf,
};

use dag::authority::Authority;
use orchestrator::{
    benchmark::BenchmarkParameters,
    collector::{MetricKind, MetricSpec},
    protocol::{BINARY_PATH, Protocol, ProtocolCommands, ProtocolMetrics, ProtocolParameters},
    provider::Instance,
    settings::Settings,
};
use replica::config::{LoadGeneratorConfig, PublicReplicaConfig, ReplicaParameters};
use serde::{Deserialize, Serialize};

const PUBLIC_REPLICA_CONFIG_FILENAME: &str = PublicReplicaConfig::DEFAULT_FILENAME;
const LOAD_GENERATOR_CONFIG_FILENAME: &str = LoadGeneratorConfig::DEFAULT_FILENAME;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct NodeParameters(ReplicaParameters);

impl Deref for NodeParameters {
    type Target = ReplicaParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for NodeParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c")
    }
}

impl ProtocolParameters for NodeParameters {}

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct ClientParameters(LoadGeneratorConfig);

impl Deref for ClientParameters {
    type Target = LoadGeneratorConfig;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for ClientParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.transaction_size)
    }
}

impl ProtocolParameters for ClientParameters {}

pub struct ReplicaProtocol {
    working_dir: PathBuf,
}

impl Protocol for ReplicaProtocol {
    type NodeParameters = NodeParameters;
    type ClientParameters = ClientParameters;
}

impl ProtocolCommands for ReplicaProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec!["sudo apt -y install libfontconfig1-dev"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        vec![self.working_dir.join("storage-*")]
    }

    async fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let ips = instances
            .map(|x| x.main_ip.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        let replica_parameters = parameters.node_parameters.clone();
        let replica_parameters_string = serde_yaml::to_string(&replica_parameters).unwrap();
        let replica_parameters_path = self.working_dir.join("replica-parameters.yaml");
        let upload_replica_parameters = format!(
            "echo -e '{replica_parameters_string}' > {}",
            replica_parameters_path.display()
        );

        let mut load_generator_config = parameters.client_parameters.clone();
        load_generator_config.0.load = parameters.load / parameters.nodes;
        let load_generator_config_string = serde_yaml::to_string(&load_generator_config).unwrap();
        let load_generator_config_path = self.working_dir.join(LOAD_GENERATOR_CONFIG_FILENAME);
        let upload_load_generator_config = format!(
            "echo -e '{load_generator_config_string}' > {}",
            load_generator_config_path.display()
        );

        let genesis = [
            &format!("./{BINARY_PATH}/replica"),
            "test-genesis",
            &format!(
                "--ips {ips} --working-directory {} --replica-parameters-path {}",
                self.working_dir.display(),
                replica_parameters_path.display(),
            ),
        ]
        .join(" ");

        [
            "source $HOME/.cargo/env",
            &upload_replica_parameters,
            &upload_load_generator_config,
            &genesis,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let authority = Authority::from(i);
                let public_config_path = self.working_dir.join(PUBLIC_REPLICA_CONFIG_FILENAME);
                let private_config_path = self
                    .working_dir
                    .join(format!("private-replica-config-{authority}.yaml"));
                let load_generator_config_path =
                    self.working_dir.join(LOAD_GENERATOR_CONFIG_FILENAME);

                let run = [
                    &format!("./{BINARY_PATH}/replica"),
                    "run",
                    &format!("--authority {authority}"),
                    &format!("--public-config-path {}", public_config_path.display()),
                    &format!("--private-config-path {}", private_config_path.display()),
                    &format!(
                        "--load-generator-config-path {}",
                        load_generator_config_path.display()
                    ),
                ]
                .join(" ");

                let command = ["source $HOME/.cargo/env", &run].join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        vec![]
    }
}

impl ProtocolMetrics for ReplicaProtocol {
    fn metrics(&self) -> Vec<MetricSpec> {
        vec![
            MetricSpec {
                // Exposed as a counter but represents elapsed seconds — querying
                // `rate(benchmark_duration[1m])` always returns ~1.0, useless.
                // The cumulative value is what aggregation actually needs.
                name: dag::metrics::BENCHMARK_DURATION.into(),
                kind: MetricKind::Gauge,
            },
            MetricSpec {
                name: "latency_s".into(),
                kind: MetricKind::Histogram,
            },
            MetricSpec {
                name: "latency_s_count".into(),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: "latency_s_sum".into(),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: dag::metrics::LATENCY_SQUARED_S.into(),
                kind: MetricKind::Counter,
            },
        ]
    }

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let (ips, instances): (_, Vec<_>) = instances
            .into_iter()
            .map(|x| (IpAddr::V4(x.main_ip), x))
            .unzip();

        let public_config = PublicReplicaConfig::new_for_benchmarks(ips);
        let metrics_paths = public_config
            .all_metric_addresses()
            .map(|x| format!("{x}{}", replica::prometheus::METRICS_ROUTE));

        instances.into_iter().zip(metrics_paths).collect()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // NOTE: Hack to avoid clients metrics.
        self.nodes_metrics_path(instances, parameters)
    }
}

impl ReplicaProtocol {
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}
