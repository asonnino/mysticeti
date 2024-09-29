// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};

use super::aux_config::AuxNodeParameters;
use crate::{
    aux_node::aux_core::AuxCore,
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    metrics::Metrics,
    prometheus,
    runtime::{JoinError, JoinHandle},
    transactions_generator::TransactionGenerator,
    types::{format_authority_index, AuthorityIndex},
};

pub struct AuxiliaryValidator {
    core_handle: JoinHandle<()>,
    metrics_handle: JoinHandle<Result<(), hyper::Error>>,
}

impl AuxiliaryValidator {
    pub async fn start(
        authority: AuthorityIndex,
        core_committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
        aux_node_parameters: AuxNodeParameters,
    ) -> Result<Self> {
        tracing::info!(
            "Starting aux validator {}",
            format_authority_index(authority)
        );
        let signer = Arc::new(private_config.keypair.clone());

        // Load addresses.
        let network_address = public_config
            .network_address(authority)
            .ok_or(eyre!("No network address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_network_address = network_address;
        binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        let metrics_address = public_config
            .metrics_address(authority)
            .ok_or(eyre!("No metrics address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Boot the prometheus server.
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry, Some(&core_committee));
        reporter.start();
        let metrics_handle =
            prometheus::start_prometheus_server(binding_metrics_address, &registry);

        // Boot the transaction generator.
        let (tx_sender, tx_receiver) = tokio::sync::mpsc::channel(1000);
        TransactionGenerator::start(
            tx_sender,
            authority,
            client_parameters,
            public_config.clone(),
            metrics.clone(),
        );

        // Boot core
        let core_handle = tokio::spawn(async move {
            AuxCore::new(
                core_committee,
                authority,
                signer,
                public_config.clone(),
                aux_node_parameters,
            )
            .run(tx_receiver)
            .await;
        });

        tracing::info!("Validator {authority} listening on {network_address}");
        tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

        Ok(Self {
            core_handle,
            metrics_handle,
        })
    }

    pub async fn await_completion(
        self,
    ) -> (
        Result<(), JoinError>,
        Result<Result<(), hyper::Error>, JoinError>,
    ) {
        tokio::join!(self.core_handle, self.metrics_handle)
    }
}

#[cfg(test)]
mod tests {
    // use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration};

    // use tempdir::TempDir;
    // use tokio::time;

    // use super::AuxiliaryValidator;
    // use crate::{
    //     committee::Committee,
    //     config::{self, ClientParameters, NodeParameters, NodePrivateConfig, NodePublicConfig},
    //     types::AuthorityIndex,
    //     validator::{smoke_tests::check_commit, Validator},
    // };

    // /// Await for all the validators specified by their metrics addresses to commit.
    // async fn await_for_aux_block_commits(addresses: Vec<SocketAddr>) {
    //     let mut queue = VecDeque::from(addresses);
    //     while let Some(address) = queue.pop_front() {
    //         time::sleep(Duration::from_millis(100)).await;
    //         match check_commit(&address).await {
    //             Ok(commits) if commits => (),
    //             _ => queue.push_back(address),
    //         }
    //     }
    // }

    // #[tokio::test]
    // #[tracing_test::traced_test]
    // async fn auxiliary_blocks_commit() {
    //     let committee_size = 5;
    //     let node_parameters = NodeParameters {
    //         max_core_committee_size: 4,
    //         auxiliary_validators_liveness_threshold: 1,
    //         auxiliary_blocks_inclusion_period: 10,
    //         ..NodeParameters::default()
    //     };
    //     let committee = Committee::new_for_benchmarks(committee_size, &node_parameters);
    //     let public_config =
    //         NodePublicConfig::new_for_tests_with_parameters(committee_size, node_parameters)
    //             .with_port_offset(0);
    //     let client_parameters = ClientParameters::new_for_tests();

    //     let dir = TempDir::new("auxiliary_blocks_commit").unwrap();
    //     let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    //     private_configs.iter().for_each(|private_config| {
    //         fs::create_dir_all(&private_config.storage_path).unwrap();
    //     });

    //     for (i, private_config) in private_configs.into_iter().enumerate() {
    //         let authority = i as AuthorityIndex;
    //         if public_config.is_core_validator(authority) {
    //             Validator::start(
    //                 authority,
    //                 committee.clone(),
    //                 public_config.clone(),
    //                 private_config,
    //                 client_parameters.clone(),
    //             )
    //             .await
    //             .unwrap();
    //         } else {
    //             AuxiliaryValidator::start(
    //                 authority,
    //                 committee.clone(),
    //                 public_config.clone(),
    //                 private_config,
    //                 client_parameters.clone(),
    //             )
    //             .await
    //             .unwrap();
    //         }
    //     }

    //     let addresses = public_config
    //         .all_metric_addresses()
    //         .map(|address| address.to_owned())
    //         .collect();
    //     let timeout = config::node_defaults::default_leader_timeout() * 5;

    //     tokio::select! {
    //         _ = await_for_aux_block_commits(addresses) => (),
    //         _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
    //     }
    // }
}
