// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};

use crate::{
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    context::TokioCtx,
    core::{
        block_handler::{CommitHandler, RealBlockHandler},
        Core, CoreOptions,
    },
    metrics::{self, Metrics},
    net_sync::NetworkSyncer,
    network::Network,
    storage::Storage,
    transactions_generator::TransactionGenerator,
    types::AuthorityIndex,
};

pub struct Validator {
    network_synchronizer: NetworkSyncer<TokioCtx>,
    metrics_handle: tokio::task::JoinHandle<()>,
}

impl Validator {
    pub async fn start(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
    ) -> Result<Self> {
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
        let metrics = Metrics::new(&registry, committee.len(), None);

        let metrics_handle =
            metrics::server::start_prometheus_server(binding_metrics_address, &registry);

        // Open storage.
        let (storage, recovered) =
            Storage::open(authority, private_config.wal(), metrics.clone(), &committee)
                .expect("Failed to open storage");

        // Boot the validator node.
        let (block_handler, block_sender) = RealBlockHandler::new(metrics.clone());

        TransactionGenerator::<TokioCtx>::start(
            block_sender,
            authority,
            client_parameters,
            public_config.clone(),
            metrics.clone(),
        );
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let core = Core::open(
            block_handler,
            authority,
            committee.clone(),
            private_config,
            &public_config,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::default(),
        );
        let network = Network::load(
            &public_config,
            authority,
            binding_network_address,
            metrics.clone(),
        )
        .await;
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            public_config.parameters.wave_length,
            commit_handler,
            metrics,
            &public_config,
        );

        tracing::info!("Validator {authority} listening on {network_address}");
        tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

        Ok(Self {
            network_synchronizer,
            metrics_handle,
        })
    }

    pub async fn await_completion(
        self,
    ) -> (
        Result<(), tokio::task::JoinError>,
        Result<(), tokio::task::JoinError>,
    ) {
        tokio::join!(
            self.network_synchronizer.await_completion(),
            self.metrics_handle
        )
    }

    pub async fn stop(self) {
        self.network_synchronizer.shutdown().await;
    }
}

#[cfg(test)]
mod smoke_tests {
    use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration};

    use tempdir::TempDir;
    use tokio::time;

    use super::Validator;
    use crate::{
        committee::Committee,
        config::{self, ClientParameters, NodePrivateConfig, NodePublicConfig},
        metrics,
        types::AuthorityIndex,
    };

    /// Check whether the validator specified by its metrics address has committed at least once.
    async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
        let route = metrics::server::METRICS_ROUTE;
        let res = reqwest::get(format! {"http://{address}{route}"}).await?;
        let string = res.text().await?;
        let commit = string.contains("committed_leaders_total");
        Ok(commit)
    }

    /// Await for all the validators specified by their metrics addresses to commit.
    async fn await_for_commits(addresses: Vec<SocketAddr>) {
        let mut queue = VecDeque::from(addresses);
        while let Some(address) = queue.pop_front() {
            time::sleep(Duration::from_millis(100)).await;
            match check_commit(&address).await {
                Ok(commits) if commits => (),
                _ => queue.push_back(address),
            }
        }
    }

    /// Ensure that a committee of honest validators commits.
    #[tokio::test]
    async fn validator_commit() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(0);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_commit").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        for (i, private_config) in private_configs.into_iter().enumerate() {
            let authority = i as AuthorityIndex;

            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 5;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    /// Ensure validators can sync missing blocks
    #[tokio::test]
    async fn validator_sync() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(100);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_sync").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        // Boot all validators but one.
        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }
            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        // Boot the last validator after they others commit.
        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }

        // Boot the last validator.
        let authority = 0;
        let private_config =
            NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size).remove(authority);
        let validator = Validator::start(
            authority as AuthorityIndex,
            committee.clone(),
            public_config.clone(),
            private_config,
            client_parameters,
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());

        // Ensure the last validator commits.
        let address = public_config
            .all_metric_addresses()
            .next()
            .map(|address| address.to_owned())
            .unwrap();
        let timeout = config::node_defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(vec![address]) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    // Ensure that honest validators commit despite the presence of a crash fault.
    #[tokio::test]
    async fn validator_crash_faults() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(200);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_crash_faults").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }

            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 15;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }
}
