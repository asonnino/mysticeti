// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, net::SocketAddr, sync::Arc};

use tokio::{
    sync::{mpsc, Notify},
    task::JoinHandle,
};

use super::aux_block_store::AuxiliaryBlockStore;
use crate::{
    aux_networking::server::{Connection, NetworkServer},
    aux_node::{aux_config::AuxiliaryCommittee, aux_message::PartialAuxiliaryCertificate},
    block_handler::BlockHandler,
    block_store::BlockStore,
    committee::Committee,
    core_thread::CoreThreadDispatcher,
    crypto::Signer,
    data::Data,
    network::NetworkMessage,
    syncer::CommitObserver,
    types::AuthorityIndex,
};

struct AuxHelperServerInner<H: BlockHandler, C: CommitObserver> {
    authority: AuthorityIndex,
    signer: Arc<Signer>,
    core_committee: Arc<Committee>,
    aux_committee: Arc<AuxiliaryCommittee>,
    block_store: BlockStore,
    aux_block_store: AuxiliaryBlockStore,
    syncer: CoreThreadDispatcher<H, Arc<Notify>, C>,
}

pub struct AuxHelperServer {
    _server_handle: JoinHandle<io::Result<()>>,
}

impl AuxHelperServer {
    pub const DEFAULT_AUX_SERVER_CHANNEL_SIZE: usize = 1024;

    pub async fn start<H, C>(
        server_address: SocketAddr,
        authority: AuthorityIndex,
        signer: Arc<Signer>,
        core_committee: Arc<Committee>,
        aux_committee: Arc<AuxiliaryCommittee>,
        block_store: BlockStore,
        aux_block_store: AuxiliaryBlockStore,
        syncer: CoreThreadDispatcher<H, Arc<Notify>, C>,
    ) -> Self
    where
        H: BlockHandler + 'static,
        C: CommitObserver + 'static,
    {
        tracing::info!("[{authority}] Starting aux helper server on {server_address}");

        let inner = Arc::new(AuxHelperServerInner {
            authority,
            signer,
            core_committee,
            aux_committee,
            block_store,
            aux_block_store,
            syncer,
        });

        let (tx_connections, mut rx_connections) =
            mpsc::channel(Self::DEFAULT_AUX_SERVER_CHANNEL_SIZE);
        let server_handle = NetworkServer::new(server_address, tx_connections).spawn();

        while let Some(connection) = rx_connections.recv().await {
            tokio::spawn(Self::handle_connection(inner.clone(), connection));
        }

        Self {
            _server_handle: server_handle,
        }
    }

    async fn handle_connection<H, C>(
        inner: Arc<AuxHelperServerInner<H, C>>,
        mut connection: Connection<NetworkMessage, NetworkMessage>,
    ) where
        H: BlockHandler + 'static,
        C: CommitObserver + 'static,
    {
        let id = inner.authority;
        let peer = connection.peer;
        tracing::info!("[{id}] Received connection from {peer}");

        while let Some(message) = connection.recv().await {
            match message {
                // Respond to an aux block request with a vote.
                NetworkMessage::AuxiliaryBlock(block) => {
                    let reference = block.reference().clone();
                    tracing::debug!("[{id}] Received auxiliary block {reference} from {peer}");

                    // Verify the auxiliary block.
                    if let Err(e) =
                        block.verify_auxiliary(&inner.core_committee, &inner.aux_committee)
                    {
                        tracing::warn!("[{id}] Rejected incorrect block {reference} from auxiliary validator {peer}: {e:?}");
                        break;
                    }

                    // Do not sign conflicting blocks for this authority.
                    if !inner.aux_block_store.safe_to_vote(&reference) {
                        tracing::warn!("[{id}] Received conflicting block from {peer}");
                        break;
                    }

                    // Ensure we have the references. Otherwise, request them from the sender.
                    let missing: Vec<_> = block
                        .includes()
                        .iter()
                        .cloned()
                        .filter(|&parent| !inner.block_store.block_exists(parent))
                        .collect();
                    if !missing.is_empty() {
                        tracing::debug!("[{id}] Received aux block {reference:?} with missing dependencies {missing:?}");
                        if let Err(e) = connection
                            .send(NetworkMessage::RequestBlocks(missing))
                            .await
                        {
                            tracing::warn!("[{id}] Failed to send sync request to {peer}: {e:?}");
                        }
                        break;
                    }

                    // Add block to auxiliary block store.
                    let author = block.author();
                    inner.aux_block_store.add_auxiliary_block(block);

                    // Tell the core to check whether this block unblocks other pending blocks.
                    inner.syncer.notify_aux_block_reception(reference).await;

                    // Reply with a vote if the sender is an auxiliary validator.
                    if !inner.aux_committee.exists(author) {
                        break;
                    }

                    let vote = PartialAuxiliaryCertificate::from_block_reference(
                        reference,
                        id,
                        &inner.signer,
                    );
                    let data = Data::new(vote);

                    // Send back the vote.
                    if let Err(e) = connection.send(NetworkMessage::AuxiliaryVote(data)).await {
                        tracing::warn!("[{id}] Failed to send auxiliary vote to {peer}: {e:?}");
                        break;
                    }
                }

                // Wait for a certificate and then return.
                NetworkMessage::AuxiliaryCertificate(certificate) => {
                    let reference = certificate.block_reference;
                    tracing::debug!(
                        "[{id}] Received auxiliary certificate {reference} from {peer}",
                    );

                    // Verify the auxiliary certificate.
                    if let Err(e) = certificate.verify(&inner.core_committee) {
                        tracing::warn!("[{id}] Rejected incorrect block {reference} from auxiliary validator {peer}: {e:?}");
                        break;
                    }

                    // Ensure we have the certified block data, otherwise drop the certificate.
                    let Some(includes) = inner.aux_block_store.get_block_includes(&reference)
                    else {
                        tracing::debug!(
                            "[{id}] Received aux certificate over missing block {reference:?}"
                        );
                        if let Err(e) = connection
                            .send(NetworkMessage::RequestBlocks(vec![reference]))
                            .await
                        {
                            tracing::warn!("[{id}] Failed to send sync request to {peer}: {e:?}");
                        }
                        break;
                    };

                    // Ensure we have the references. Otherwise, request them from the sender.
                    let missing: Vec<_> = includes
                        .into_iter()
                        .filter(|&parent| !inner.block_store.block_exists(parent))
                        .collect();
                    if !missing.is_empty() {
                        tracing::debug!("[{id}] Received aux block {reference:?} with missing dependencies {missing:?}");
                        if let Err(e) = connection
                            .send(NetworkMessage::RequestBlocks(missing))
                            .await
                        {
                            tracing::warn!("[{id}] Failed to send sync request to {peer}: {e:?}");
                        }
                        break;
                    }

                    // Add the the certificate to the auxiliary block store. Flag the block as certified and
                    // thus ready for inclusion as a weak link.
                    inner.aux_block_store.add_certificate(reference);
                }

                message @ NetworkMessage::AuxiliaryVote(_)
                | message @ NetworkMessage::Block(_)
                | message @ NetworkMessage::SubscribeOwnFrom(_)
                | message @ NetworkMessage::RequestBlocks(_)
                | message @ NetworkMessage::BlockNotFound(_) => {
                    tracing::warn!(
                        "[{id}] Unexpected message {message:?} from {peer}, dropping connection"
                    );
                    break;
                }
            }
        }
    }
}
