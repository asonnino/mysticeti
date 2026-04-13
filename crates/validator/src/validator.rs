// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use ::prometheus::Registry;
use color_eyre::eyre::{Result, eyre};
use tokio::task::JoinHandle;

use consensus::universal_committer::{UniversalCommitter, UniversalCommitterBuilder};
use dag::{
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    context::TokioCtx,
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
    transactions_generator::TransactionGenerator,
    types::AuthorityIndex,
};

use crate::prometheus as metrics_server;

pub struct Validator {
    authority: AuthorityIndex,
    network_synchronizer: NetworkSyncer<TokioCtx, UniversalCommitter>,
    metrics_handle: Option<JoinHandle<()>>,
}

impl Validator {
    pub async fn await_completion(self) -> Result<()> {
        match self.metrics_handle {
            Some(metrics_handle) => {
                tokio::select! {
                    result = self.network_synchronizer
                        .await_completion() => {
                        result.map_err(|error| eyre!(
                            "Validator {} crashed: {error}",
                            self.authority
                        ))
                    }
                    result = metrics_handle => {
                        result.map_err(|error| eyre!(
                            "Metrics server for validator {} crashed: {error}",
                            self.authority
                        ))
                    }
                }
            }
            None => self
                .network_synchronizer
                .await_completion()
                .await
                .map_err(|error| eyre!("Validator {} crashed: {error}", self.authority)),
        }
    }
}

pub struct ValidatorBuilder {
    authority: AuthorityIndex,
    committee: Arc<Committee>,
    public_config: NodePublicConfig,
    private_config: NodePrivateConfig,
    client_parameters: ClientParameters,
    registry: Registry,
    metrics_server_address: Option<SocketAddr>,
}

impl ValidatorBuilder {
    pub fn new(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
        registry: Registry,
    ) -> Self {
        Self {
            authority,
            committee,
            public_config,
            private_config,
            client_parameters,
            registry,
            metrics_server_address: None,
        }
    }

    pub fn with_metrics_server(mut self, address: SocketAddr) -> Self {
        self.metrics_server_address = Some(address);
        self
    }

    pub async fn start(self) -> Result<Validator> {
        let metrics = Metrics::new(&self.registry, self.committee.len(), None);

        let metrics_handle = self
            .metrics_server_address
            .map(|address| metrics_server::start_prometheus_server(address, &self.registry));

        let network_address = self
            .public_config
            .network_address(self.authority)
            .ok_or(eyre!("No network address for authority {}", self.authority))?;
        let mut binding_address = network_address;
        binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        let (storage, recovered) = Storage::open(
            self.authority,
            self.private_config.wal(),
            metrics.clone(),
            &self.committee,
        )
        .expect("Failed to open storage");

        let (block_handler, block_sender) = RealBlockHandler::new(metrics.clone());

        TransactionGenerator::<TokioCtx>::start(
            block_sender,
            self.authority,
            self.client_parameters,
            self.public_config.clone(),
            metrics.clone(),
        );
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let committer = UniversalCommitterBuilder::new(
            self.committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
        )
        .with_number_of_leaders(self.public_config.parameters.number_of_leaders)
        .with_pipeline(self.public_config.parameters.enable_pipelining)
        .build();
        let core = Core::open(
            block_handler,
            self.authority,
            self.committee.clone(),
            self.private_config,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::default(),
            committer,
        );
        let network = Network::load(
            &self.public_config,
            self.authority,
            binding_address,
            metrics.clone(),
        )
        .await;
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            self.public_config.parameters.wave_length,
            commit_handler,
            metrics,
            &self.public_config,
        );

        Ok(Validator {
            authority: self.authority,
            network_synchronizer,
            metrics_handle,
        })
    }
}
