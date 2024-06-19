// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    net::IpAddr,
    ops::Deref,
    path::PathBuf,
};

use mysticeti_core::{
    config::{self, ClientParameters, NodeParameters},
    types::AuthorityIndex,
};
use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::{benchmark::BenchmarkParameters, client::Instance, settings::Settings};

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct MysticetiNodeParameters(NodeParameters);

impl Deref for MysticetiNodeParameters {
    type Target = NodeParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for MysticetiNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.consensus_only {
            write!(f, "c")
        } else {
            write!(f, "fpc")
        }
    }
}

impl Display for MysticetiNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.consensus_only {
            write!(f, "Consensus-only mode")
        } else {
            write!(f, "FPC mode")
        }
    }
}

impl ProtocolParameters for MysticetiNodeParameters {}

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct MysticetiClientParameters(ClientParameters);

impl Deref for MysticetiClientParameters {
    type Target = ClientParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for MysticetiClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.transaction_size)
    }
}

impl Display for MysticetiClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}B tx", self.transaction_size)
    }
}

impl ProtocolParameters for MysticetiClientParameters {}

pub struct MysticetiProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for MysticetiProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec!["sudo apt -y install libfontconfig1-dev"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        vec![self.working_dir.join("storage-*")]
    }

    async fn genesis_command<'a, I>(&self, instances: I, parameters: &BenchmarkParameters) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let ips = instances
            .map(|x| x.main_ip.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        let node_parameters = parameters.node_parameters.clone();
        let node_parameters_string = serde_yaml::to_string(&node_parameters).unwrap();
        let node_parameters_path = self.working_dir.join("node-parameters.yaml");
        let upload_node_parameters = format!(
            "echo -e '{node_parameters_string}' > {}",
            node_parameters_path.display()
        );

        let client_parameters = parameters.client_parameters.clone();
        let client_parameters_string = serde_yaml::to_string(&client_parameters).unwrap();
        let client_parameters_path = self.working_dir.join("client-parameters.yaml");
        let upload_client_parameters = format!(
            "echo -e '{client_parameters_string}' > {}",
            client_parameters_path.display()
        );

        let genesis = [
            &format!("./{BINARY_PATH}/mysticeti"),
            "benchmark-genesis",
            &format!(
                "--ips {ips} --working-directory {} --node-parameters-path {}",
                self.working_dir.display(),
                node_parameters_path.display(),
            ),
        ]
        .join(" ");

        [
            "source $HOME/.cargo/env",
            &upload_node_parameters,
            &upload_client_parameters,
            &genesis,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let authority = i as AuthorityIndex;
                let committee_path = self.working_dir.join("committee.yaml");
                let public_config_path = self.working_dir.join("public-config.yaml");
                let private_config_path = self
                    .working_dir
                    .join(format!("private-config-{authority}.yaml"));
                let client_parameters_path = self.working_dir.join("client-parameters.yaml");

                let run = [
                    &format!("./{BINARY_PATH}/mysticeti"),
                    "run",
                    &format!("--authority {authority}"),
                    &format!("--committee-path {}", committee_path.display()),
                    &format!("--public-config-path {}", public_config_path.display()),
                    &format!("--private-config-path {}", private_config_path.display()),
                    &format!(
                        "--client-parameters-path {}",
                        client_parameters_path.display()
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
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // TODO: Isolate clients from the node (#9).
        vec![]
    }
}

impl ProtocolMetrics for MysticetiProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "total_transactions";
    const LATENCY_BUCKETS: &'static str = "latency_buckets";
    const LATENCY_SUM: &'static str = "latency_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_sum";

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let (ips, instances): (_, Vec<_>) = instances
            .into_iter()
            .map(|x| (IpAddr::V4(x.main_ip), x))
            .unzip();

        let node_parameters = Some(parameters.node_parameters.deref().clone());
        let node_config = config::NodePublicConfig::new_for_benchmarks(ips, node_parameters);
        let metrics_paths = node_config
            .all_metric_addresses()
            .map(|x| format!("{x}{}", mysticeti_core::prometheus::METRICS_ROUTE));

        instances.into_iter().zip(metrics_paths).collect()
    }

    fn clients_metrics_path<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // TODO: Implement this when the client metrics are available (#9).
        vec![]
    }
}

impl MysticetiProtocol {
    /// Make a new instance of the Mysticeti protocol commands generator.
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}
