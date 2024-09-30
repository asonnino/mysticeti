// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use parking_lot::RwLock;
use tokio::sync::mpsc;

use super::{
    aux_aggregator::AuxiliaryAggregator,
    aux_config::AuxNodeParameters,
    aux_disseminator::AuxDisseminator,
    aux_message::PartialAuxiliaryCertificate,
};
use crate::{
    aux_networking::client::NetworkClient,
    aux_node::aux_disseminator::DisseminatorMessage,
    committee::Committee,
    config::NodePublicConfig,
    crypto::Signer,
    data::Data,
    network::NetworkMessage,
    runtime::timestamp_utc,
    types::{
        format_authority_index,
        AuthorityIndex,
        BaseStatement,
        BlockReference,
        RoundNumber,
        Stake,
        StatementBlock,
        Transaction,
    },
};

pub struct ThresholdClock {
    core_committee: Arc<Committee>,
    blocks: HashMap<RoundNumber, (HashSet<AuthorityIndex>, Stake, Vec<BlockReference>)>,
}

impl ThresholdClock {
    pub fn new(core_committee: Arc<Committee>) -> Self {
        Self {
            core_committee,
            blocks: HashMap::new(),
        }
    }

    pub fn update(&mut self, reference: BlockReference) -> Option<Vec<BlockReference>> {
        let tick = {
            let (authorities, stake, references) =
                self.blocks
                    .entry(reference.round)
                    .or_insert((HashSet::new(), 0, Vec::new()));

            if !authorities.insert(reference.authority) {
                return None;
            }

            *stake += self
                .core_committee
                .get_stake(reference.authority)
                .expect("Unknown authority");
            references.push(reference);

            if !self.core_committee.is_quorum(*stake) {
                return None;
            }

            Some(references.clone())
        };

        if let Some(references) = tick {
            self.blocks.retain(|r, _| r > &reference.round);
            return Some(references);
        }

        None
    }
}

#[derive(Clone)]
pub struct AuxCoreInner {
    pub authority: AuthorityIndex,
    signer: Arc<Signer>,
    core_committee: Arc<Committee>,
    threshold_clock: Arc<RwLock<ThresholdClock>>,
    aux_aggregator: AuxiliaryAggregator,
    pub blocks: Arc<RwLock<HashMap<RoundNumber, Data<StatementBlock>>>>,
    pub certificates: Arc<RwLock<HashMap<RoundNumber, Data<PartialAuxiliaryCertificate>>>>,
    pending_transactions: Arc<RwLock<Vec<BaseStatement>>>,
    aux_node_parameters: AuxNodeParameters,
}

pub struct AuxCore {
    _inner: Arc<AuxCoreInner>,
    _node_public_config: NodePublicConfig,
    _aux_node_parameters: AuxNodeParameters,
    _tx_new_disseminator: mpsc::Sender<mpsc::Sender<DisseminatorMessage>>,
    _tx_notification: mpsc::Sender<DisseminatorMessage>,
}

impl AuxCore {
    pub async fn start(
        core_committee: Arc<Committee>,
        authority: AuthorityIndex,
        signer: Arc<Signer>,
        node_public_config: NodePublicConfig,
        aux_node_parameters: AuxNodeParameters,
        mut transactions_receiver: mpsc::Receiver<Vec<Transaction>>,
    ) -> Self {
        let inner = Arc::new(AuxCoreInner {
            authority,
            signer,
            core_committee: core_committee.clone(),
            threshold_clock: Arc::new(RwLock::new(ThresholdClock::new(core_committee.clone()))),
            aux_aggregator: AuxiliaryAggregator::new(core_committee.clone()),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            certificates: Arc::new(RwLock::new(HashMap::new())),
            pending_transactions: Arc::new(RwLock::new(Vec::new())),
            aux_node_parameters: aux_node_parameters.clone(),
        });

        // Collect transactions.
        let inner_clone = inner.clone();
        let max_pending_transactions = aux_node_parameters.max_block_size * 10;
        tokio::spawn(async move {
            while let Some(transactions) = transactions_receiver.recv().await {
                let mut guard = inner_clone.pending_transactions.write();
                // Drop transactions if the buffer is full. (Auxiliary validators are unreliable.)
                if guard.len() < max_pending_transactions {
                    guard.extend(transactions.into_iter().map(BaseStatement::Share));
                }
            }
        });

        // Drive dissemination controller.
        let (tx_new_disseminator, rx_new_disseminator) = mpsc::channel(100);
        let (tx_notification, rx_notification) = mpsc::channel(100);
        tokio::spawn(Self::dissemination_controller(
            rx_new_disseminator,
            rx_notification,
        ));

        // Drive networking.
        for (peer, address) in node_public_config.all_aux_helper_addresses().enumerate() {
            tracing::debug!("[{authority}] Starting connection with {peer} ({address})");
            let (tx_send_through_network, rx_send_through_network) = mpsc::channel(1000);
            let (tx_receive_from_network, rx_receive_from_network) = mpsc::channel(1000);
            NetworkClient::new(
                authority as usize,
                address,
                tx_receive_from_network,
                rx_send_through_network,
            )
            .spawn();

            let (disseminator, notifier) = AuxDisseminator::new(
                tx_send_through_network.clone(),
                inner.clone(),
                peer as AuthorityIndex,
            );
            tokio::spawn(async move { disseminator.run().await });
            tx_new_disseminator.send(notifier).await.ok();

            let tx_notification_clone = tx_notification.clone();
            let inner_clone = inner.clone();
            tokio::spawn(async move {
                Self::connection_handler(
                    peer as AuthorityIndex,
                    inner_clone,
                    tx_send_through_network,
                    rx_receive_from_network,
                    tx_notification_clone,
                )
                .await;
            });
        }

        Self {
            _inner: inner,
            _node_public_config: node_public_config,
            _aux_node_parameters: aux_node_parameters,
            _tx_new_disseminator: tx_new_disseminator,
            _tx_notification: tx_notification,
        }
    }

    async fn dissemination_controller(
        mut rx_new_disseminator: mpsc::Receiver<mpsc::Sender<DisseminatorMessage>>,
        mut rx_notification: mpsc::Receiver<DisseminatorMessage>,
    ) {
        let mut disseminators = Vec::new();
        loop {
            tokio::select! {
                Some(tx_disseminator) = rx_new_disseminator.recv() => {
                    disseminators.push(tx_disseminator);
                }
                Some(notification) = rx_notification.recv() => {
                    for tx_disseminator in &disseminators {
                        tx_disseminator.send(notification.clone()).await.ok();
                    }
                }
                else => {
                    tracing::warn!("Dissemination controller terminated");
                    break;
                }
            }
        }
    }

    async fn connection_handler(
        peer: AuthorityIndex,
        inner: Arc<AuxCoreInner>,
        tx_send_through_network: mpsc::Sender<NetworkMessage>,
        mut rx_receive_from_network: mpsc::Receiver<NetworkMessage>,
        tx_notification: mpsc::Sender<DisseminatorMessage>,
    ) {
        let core_committee = &inner.core_committee;
        let mut last_created_block = None;

        while let Some(message) = rx_receive_from_network.recv().await {
            match message {
                NetworkMessage::Block(block) => {
                    if !inner.aux_node_parameters.inclusion_round(block.round()) {
                        // Drop the block
                    }

                    let reference = block.reference();
                    tracing::debug!(
                        "{} Received {reference} from {peer}",
                        format_authority_index(inner.authority)
                    );
                    if let Err(e) = block.verify(&inner.core_committee) {
                        tracing::warn!("Rejected incorrect block {reference} from {peer}: {e:?}");
                        // todo: drop connection
                    }

                    let round = if let Some(references) =
                        inner.threshold_clock.write().update(*reference)
                    {
                        let max_block_size = inner.aux_node_parameters.max_block_size;
                        let transactions = {
                            let mut guard = inner.pending_transactions.write();
                            if guard.len() > max_block_size {
                                guard.drain(..max_block_size).collect()
                            } else {
                                guard.drain(..).collect()
                            }
                        };

                        let block = StatementBlock::new_with_signer(
                            inner.authority,
                            block.round() + 1,
                            references,
                            Vec::new(), // aux includes
                            transactions,
                            timestamp_utc().as_nanos(),
                            false, // epoch_marker
                            &inner.signer,
                        );
                        tracing::debug!(
                            "{} Created block {:?}",
                            format_authority_index(inner.authority),
                            block.reference()
                        );
                        let round = block.round();
                        let data = Data::new(block);
                        last_created_block = Some(data.clone());
                        inner.blocks.write().insert(round, data);
                        Some(round)
                    } else {
                        None
                    };

                    // Notify block disseminator.
                    if let Some(round) = round {
                        tx_notification
                            .send(DisseminatorMessage::NotifyAuxBlock(round))
                            .await
                            .ok();
                    }
                }
                NetworkMessage::RequestBlocks(references) => {
                    tracing::debug!(
                        "{} Received RequestBlocks {:?} from {peer}",
                        format_authority_index(inner.authority),
                        references
                    );

                    // Send requested blocks.
                    let mut blocks = Vec::new();
                    let guard = inner.blocks.read();
                    for reference in references {
                        if let Some(block) = inner.blocks.read().get(&reference.round) {
                            blocks.push(block.clone());
                        }
                    }
                    drop(guard);
                    for block in blocks {
                        tx_send_through_network
                            .send(NetworkMessage::AuxiliaryBlock(block.clone()))
                            .await
                            .ok();
                    }

                    // Send again the last created block.
                    if let Some(ref block) = last_created_block {
                        tx_send_through_network
                            .send(NetworkMessage::AuxiliaryBlock(block.clone()))
                            .await
                            .ok();
                    }
                }
                NetworkMessage::AuxiliaryVote(vote) => {
                    tracing::debug!(
                        "{} Received vote over {} from validator {peer}",
                        format_authority_index(inner.authority),
                        vote.block_reference
                    );

                    // Verify the vote.
                    if let Err(e) = vote.verify(core_committee) {
                        tracing::warn!(
                            "Rejected incorrect vote over {} from validator {peer}: {e:?}",
                            vote.block_reference
                        );
                        // todo: drop connection
                    }

                    // Add the partial vote to the auxiliary votes aggregator. If there is a certificate,
                    // the authority will broadcast it in the same task that sends blocks.
                    if let Some(certificate) = inner.aux_aggregator.add_vote(vote.deref().clone()) {
                        tracing::debug!(
                            "{} Created certificate for round {}",
                            format_authority_index(inner.authority),
                            certificate.block_reference.round
                        );

                        let round = certificate.block_reference.round;
                        let data = Data::new(certificate);
                        inner.certificates.write().insert(round, data);

                        // Notify disseminator of new certificate.
                        tx_notification
                            .send(DisseminatorMessage::NotifyAuxCertificate(round))
                            .await
                            .ok();
                    }
                }
                NetworkMessage::SubscribeOwnFrom(_) => {
                    // Ignore
                }
                NetworkMessage::BlockNotFound(_) => {
                    // Ignore
                }
                NetworkMessage::AuxiliaryBlock(_) => {
                    // Ignore
                }
                NetworkMessage::AuxiliaryCertificate(_) => {
                    // Ignore
                }
            }
        }
    }
}
