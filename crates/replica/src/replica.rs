// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use ::prometheus::Registry;
use color_eyre::eyre::{Result, eyre};
use tokio::{sync::mpsc, task::JoinHandle};

use consensus::protocols::mysticeti::Mysticeti;
use dag::{
    block::types::Transaction,
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
    types::AuthorityIndex,
};

use crate::generator::TransactionGenerator;

use crate::prometheus as metrics_server;

pub struct Replica {
    authority: AuthorityIndex,
    committee: Arc<Committee>,
    public_config: NodePublicConfig,
    private_config: NodePrivateConfig,
    registry: Registry,
    metrics_server_address: Option<SocketAddr>,
    client_parameters: Option<ClientParameters>,
}

impl Replica {
    pub(crate) fn new(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        registry: Registry,
        metrics_server_address: Option<SocketAddr>,
        client_parameters: Option<ClientParameters>,
    ) -> Self {
        Self {
            authority,
            committee,
            public_config,
            private_config,
            registry,
            metrics_server_address,
            client_parameters,
        }
    }

    #[tracing::instrument(skip_all, fields(authority = self.authority))]
    pub async fn run(self) -> Result<ReplicaHandle> {
        let metrics = Metrics::new(&self.registry, self.committee.len(), None);

        // Start the metrics HTTP server if configured.
        let metrics_handle = self
            .metrics_server_address
            .map(|address| metrics_server::start_prometheus_server(address, &self.registry));

        // Resolve the network binding address.
        let network_address = self
            .public_config
            .network_address(self.authority)
            .ok_or(eyre!("No network address for authority {}", self.authority))?;
        let mut binding_address = network_address;
        binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Open storage and recover state.
        let (storage, recovered) = Storage::open(
            self.authority,
            self.private_config.wal(),
            metrics.clone(),
            &self.committee,
        )
        .expect("Failed to open storage");

        // Set up block handling.
        let (block_handler, block_sender) = RealBlockHandler::<TokioCtx>::new(metrics.clone());

        // Start the load generator or expose the tx channel.
        let tx_sender = match self.client_parameters {
            Some(params) => {
                TransactionGenerator::start(
                    block_sender,
                    self.authority,
                    params,
                    self.public_config.parameters.max_block_size,
                    metrics.clone(),
                );
                None
            }
            None => Some(block_sender),
        };

        // Build the committer and core.
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let committer = Mysticeti::new(
            self.committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
            self.public_config.parameters.number_of_leaders,
        );
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

        // Bind the network and start the synchronizer.
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

        Ok(ReplicaHandle {
            authority: self.authority,
            network_synchronizer,
            metrics_handle,
            tx_sender,
        })
    }
}

pub struct ReplicaHandle {
    authority: AuthorityIndex,
    network_synchronizer: NetworkSyncer<TokioCtx, Mysticeti>,
    metrics_handle: Option<JoinHandle<()>>,
    tx_sender: Option<mpsc::Sender<Vec<Transaction>>>,
}

impl ReplicaHandle {
    pub fn stop(&self) {
        // TODO: propagate stop signal into Core/Syncer for
        // graceful drain of in-flight blocks.
    }

    pub async fn join(self) -> Result<()> {
        let metrics_future = async {
            match self.metrics_handle {
                Some(handle) => handle.await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            result = self.network_synchronizer
                .await_completion() => {
                result.map_err(|error| eyre!(
                    "Replica {} crashed: {error}", self.authority
                ))
            }
            result = metrics_future => {
                result.map_err(|error| eyre!(
                    "Metrics server for replica {} crashed: {error}",
                    self.authority
                ))
            }
        }
    }

    pub async fn submit(&self, transactions: Vec<Transaction>) -> Result<()> {
        let sender = self
            .tx_sender
            .as_ref()
            .ok_or(eyre!("Cannot submit: load generator is active"))?;
        sender
            .send(transactions)
            .await
            .map_err(|_| eyre!("Transaction channel closed"))
    }
}
