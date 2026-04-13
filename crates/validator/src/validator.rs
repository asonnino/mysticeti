// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use eyre::{Context, Result, eyre};
use prometheus::Registry;

use consensus::universal_committer::{UniversalCommitter, UniversalCommitterBuilder};
use dag::{
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    context::TokioCtx,
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    metrics::{self, Metrics},
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
    transactions_generator::TransactionGenerator,
    types::AuthorityIndex,
};

pub struct Validator {
    network_synchronizer: NetworkSyncer<TokioCtx, UniversalCommitter>,
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

        let registry = Registry::new();
        let metrics = Metrics::new(&registry, committee.len(), None);

        let metrics_handle =
            metrics::server::start_prometheus_server(binding_metrics_address, &registry);

        let (storage, recovered) =
            Storage::open(authority, private_config.wal(), metrics.clone(), &committee)
                .expect("Failed to open storage");

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
        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
        )
        .with_number_of_leaders(public_config.parameters.number_of_leaders)
        .with_pipeline(public_config.parameters.enable_pipelining)
        .build();
        tracing::info!(
            "Pipeline enabled: {}",
            public_config.parameters.enable_pipelining
        );
        tracing::info!(
            "Number of leaders: {}",
            public_config.parameters.number_of_leaders
        );
        let core = Core::open(
            block_handler,
            authority,
            committee.clone(),
            private_config,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::default(),
            committer,
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
}
