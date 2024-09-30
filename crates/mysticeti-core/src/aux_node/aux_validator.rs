// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};

use super::aux_config::AuxNodePublicConfig;
use crate::{
    aux_node::aux_core::AuxCore,
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    metrics::Metrics,
    prometheus,
    runtime::{JoinError, JoinHandle},
    transactions_generator::TransactionGenerator,
    types::AuthorityIndex,
};

pub struct AuxiliaryValidator {
    _core_handle: AuxCore,
    metrics_handle: JoinHandle<Result<(), hyper::Error>>,
}

impl AuxiliaryValidator {
    pub async fn start(
        authority: AuthorityIndex,
        core_committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
        aux_public_config: AuxNodePublicConfig,
    ) -> Result<Self> {
        tracing::info!("Starting aux validator {authority}",);
        let signer = Arc::new(private_config.keypair.clone());

        // Load addresses.
        let metrics_address = aux_public_config
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
        let core_handle = AuxCore::start(
            core_committee,
            authority,
            signer,
            public_config.clone(),
            aux_public_config.parameters,
            tx_receiver,
        )
        .await;

        tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

        Ok(Self {
            _core_handle: core_handle,
            metrics_handle,
        })
    }

    pub async fn await_completion(self) -> Result<Result<(), hyper::Error>, JoinError> {
        self.metrics_handle.await
    }
}

#[cfg(test)]
mod tests {

    use std::{net::SocketAddr, sync::Arc};

    use futures::future::join_all;
    use tempdir::TempDir;
    use tokio::sync::mpsc;

    use crate::{
        aux_networking::server::NetworkServer,
        aux_node::{
            aux_config::AuxNodePublicConfig,
            aux_message::PartialAuxiliaryCertificate,
            aux_validator::AuxiliaryValidator,
        },
        committee::Committee,
        config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
        crypto::{BlockDigest, Signer},
        data::Data,
        network::NetworkMessage,
        runtime::timestamp_utc,
        types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
    };

    pub fn make_core_blocks(round: RoundNumber, core_committee_size: usize) -> Vec<StatementBlock> {
        let dir = TempDir::new("make_core_blocks").unwrap();
        let core_private_configs =
            NodePrivateConfig::new_for_benchmarks(dir.as_ref(), core_committee_size);
        let references: Vec<_> = (0..core_committee_size)
            .map(|i| BlockReference {
                authority: i as AuthorityIndex,
                round: round - 1,
                digest: BlockDigest([0; 32]),
            })
            .collect();

        core_private_configs
            .into_iter()
            .enumerate()
            .map(|(authority, private_config)| {
                let signer = Arc::new(private_config.keypair.clone());
                StatementBlock::new_with_signer(
                    authority as AuthorityIndex,
                    round,
                    references.clone(),
                    vec![], // aux includes
                    vec![], // no transactions
                    timestamp_utc().as_nanos(),
                    false, // epoch_marker
                    &signer,
                )
            })
            .collect()
    }

    pub async fn mock_core_validator(
        authority_index: AuthorityIndex,
        core_committee: Arc<Committee>,
        signer: Arc<Signer>,
        server_address: SocketAddr,
        // Block to send as soon as the connection is established.
        block: StatementBlock,
    ) {
        let id = server_address;
        let (tx_connections, mut rx_connections) = mpsc::channel(10);
        let _handle =
            NetworkServer::new(authority_index as usize, server_address, tx_connections).spawn();

        // Wait for the aux validator to establish a connection.
        let mut connection = rx_connections.recv().await.unwrap();

        // Send the block
        tracing::debug!("[{id}] Sending block to {}", connection.peer);
        let data = NetworkMessage::Block(Data::new(block.clone()));
        connection.send(data).await.unwrap();

        // listen for messages
        let peer = connection.peer;
        while let Some(message) = connection.recv().await {
            match message {
                // Respond to an aux block request with a vote.
                NetworkMessage::AuxiliaryBlock(block) => {
                    let reference = block.reference();
                    tracing::debug!("[{id}] Received block {reference} from {peer}");

                    let vote = PartialAuxiliaryCertificate::from_block_reference(
                        reference.clone(),
                        authority_index,
                        &signer,
                    );
                    let message = NetworkMessage::AuxiliaryVote(Data::new(vote));
                    connection.send(message).await.unwrap();
                }

                // Wait for a certificate and then return.
                NetworkMessage::AuxiliaryCertificate(certificate) => {
                    let reference = certificate.block_reference;
                    tracing::debug!(
                        "[{id}] Received auxiliary certificate {reference} from {peer}",
                    );
                    certificate
                        .verify(&core_committee)
                        .expect("invalid certificate");

                    return;
                }

                NetworkMessage::AuxiliaryVote(_)
                | NetworkMessage::Block(_)
                | NetworkMessage::SubscribeOwnFrom(_)
                | NetworkMessage::RequestBlocks(_)
                | NetworkMessage::BlockNotFound(_) => panic!("Unexpected message"),
            }
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn create_auxiliary_blocks() {
        let core_committee_size = 4;
        let core_committee = Committee::new_for_benchmarks(core_committee_size);
        let public_config =
            NodePublicConfig::new_for_tests(core_committee_size).with_port_offset(10_000);
        let client_parameters = ClientParameters::new_for_tests();
        let aux_public_config = AuxNodePublicConfig::new_for_tests(1);

        // Boot mock core validators.
        let round = aux_public_config.parameters.inclusion_period;
        let blocks = make_core_blocks(round, core_committee_size);

        let dir = TempDir::new("create_auxiliary_blocks-1").unwrap();
        let core_private_configs =
            NodePrivateConfig::new_for_benchmarks(dir.as_ref(), core_committee_size);
        let mut core_handles = Vec::new();
        for ((i, core_private_config), block) in core_private_configs
            .into_iter()
            .enumerate()
            .zip(blocks.into_iter())
        {
            let core_authority = i as AuthorityIndex;
            let core_committee_clone = core_committee.clone();
            let core_signer = Arc::new(core_private_config.keypair.clone());
            let server_address = public_config.aux_helper_address(core_authority).unwrap();
            let handle = tokio::spawn(async move {
                mock_core_validator(
                    core_authority,
                    core_committee_clone,
                    core_signer,
                    server_address,
                    block,
                )
                .await;
            });
            core_handles.push(handle);
            tokio::task::yield_now().await;
        }

        // Boot an auxiliary validator.
        let aux_authority = 1000;
        let dir = TempDir::new("create_auxiliary_blocks-2").unwrap();
        let aux_private_config = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), 1001)
            .pop()
            .unwrap();

        let aux_validator = AuxiliaryValidator::start(
            aux_authority,
            core_committee,
            public_config,
            aux_private_config,
            client_parameters,
            aux_public_config.clone(),
        )
        .await
        .unwrap();

        // Wait for the auxiliary validator to complete.
        tokio::select! {
            _ = aux_validator.await_completion() => {},
            handles = join_all(core_handles) => {
                for handle in handles {
                    handle.unwrap();
                }
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        }
    }
}
