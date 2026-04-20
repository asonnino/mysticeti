// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr};

use ::prometheus::Registry;
use color_eyre::eyre::{Result, eyre};
use tokio::{sync::mpsc, task::JoinHandle};

use consensus::committer::Committer;
use dag::{
    authority::Authority,
    block::transaction::Transaction,
    context::TokioCtx,
    core::{
        Core,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    crypto::CryptoEngine,
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
};

use crate::config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig};
use crate::generator::TransactionGenerator;

use crate::prometheus as metrics_server;

pub struct Replica {
    authority: Authority,
    public_config: PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    registry: Registry,
    metrics_server_enabled: bool,
    load_generator_config: Option<LoadGeneratorConfig>,
}

impl Replica {
    pub(crate) fn new(
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
        registry: Registry,
        metrics_server_enabled: bool,
        load_generator_config: Option<LoadGeneratorConfig>,
    ) -> Self {
        Self {
            authority,
            public_config,
            private_config,
            registry,
            metrics_server_enabled,
            load_generator_config,
        }
    }

    #[tracing::instrument(skip_all, fields(authority = %self.authority))]
    pub async fn run(self) -> Result<ReplicaHandle> {
        let committee = self.public_config.committee();
        let metrics = Metrics::new(&self.registry, committee.len(), None);

        // Start the metrics HTTP server on the authority's metrics
        // address, rebound to 0.0.0.0 so it's reachable from outside.
        let metrics_handle = if self.metrics_server_enabled {
            let mut address = self
                .public_config
                .metrics_address(self.authority)
                .ok_or(eyre!("No metrics address for authority {}", self.authority))?;
            address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
            Some(metrics_server::start_prometheus_server(
                address,
                &self.registry,
            ))
        } else {
            None
        };

        // Resolve the network binding address (rebound to 0.0.0.0).
        let mut network_binding_address = self
            .public_config
            .network_address(self.authority)
            .ok_or(eyre!("No network address for authority {}", self.authority))?;
        network_binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Open storage and recover state.
        let (storage, recovered) = Storage::open(
            self.authority,
            self.private_config.wal(),
            metrics.clone(),
            &committee,
        )?;

        // Set up block handling.
        let (block_handler, block_sender) = RealBlockHandler::<TokioCtx>::new(metrics.clone());

        let parameters = &self.public_config.parameters;

        // Start the load generator or expose the tx channel.
        let tx_sender = match self.load_generator_config {
            Some(config) => {
                TransactionGenerator::start(
                    block_sender,
                    self.authority,
                    config,
                    parameters.dag.max_block_size,
                    metrics.clone(),
                );
                None
            }
            None => Some(block_sender),
        };

        // Build the committer and core.
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let protocol = parameters.consensus.to_protocol(committee.total_stake());
        let round_timeout = parameters
            .dag
            .round_timeout
            .unwrap_or_else(|| protocol.default_round_timeout());
        let enable_synchronizer = parameters.dag.enable_synchronizer;
        let fsync = parameters.dag.fsync;
        let crypto = CryptoEngine::new(self.private_config.keypair, protocol.require_crypto);
        let committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            protocol,
            metrics.clone(),
        );
        let core = Core::open(
            block_handler,
            self.authority,
            committee.clone(),
            metrics.clone(),
            storage,
            recovered,
            fsync,
            committer,
            crypto,
        );

        // Bind the network and start the synchronizer.
        let addresses: Vec<_> = self.public_config.all_network_addresses().collect();
        let network = Network::load(
            &addresses,
            self.authority,
            network_binding_address,
            metrics.clone(),
        )
        .await;
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            round_timeout,
            enable_synchronizer,
            commit_handler,
            metrics,
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
    authority: Authority,
    network_synchronizer: NetworkSyncer<TokioCtx, Committer>,
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
