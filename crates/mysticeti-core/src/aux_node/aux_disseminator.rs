// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::mpsc;

use super::aux_core::AuxCoreInner;
use crate::{
    network::NetworkMessage,
    types::{format_authority_index, AuthorityIndex, RoundNumber},
};

#[derive(Debug, Clone)]
pub enum DisseminatorMessage {
    Stop,
    NotifyAuxBlock(RoundNumber),
    NotifyAuxCertificate(RoundNumber),
}

pub struct AuxDisseminator {
    notification_receiver: mpsc::Receiver<DisseminatorMessage>,
    connection_sender: mpsc::Sender<NetworkMessage>,
    inner: Arc<AuxCoreInner>,
    peer: AuthorityIndex,
}

impl AuxDisseminator {
    pub fn new(
        connection_sender: mpsc::Sender<NetworkMessage>,
        inner: Arc<AuxCoreInner>,
        peer: AuthorityIndex,
    ) -> (Self, mpsc::Sender<DisseminatorMessage>) {
        let (notification_sender, notification_receiver) = mpsc::channel(1000);
        let disseminator = Self {
            notification_receiver,
            connection_sender,
            inner,
            peer,
        };
        (disseminator, notification_sender)
    }

    pub async fn run(mut self) {
        while let Some(message) = self.notification_receiver.recv().await {
            // Wait a bit to avoid eager synchronization.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            match message {
                DisseminatorMessage::Stop => {
                    tracing::warn!("Received Stop message, stopping AuxDisseminator");
                    break;
                }
                DisseminatorMessage::NotifyAuxBlock(round) => {
                    tracing::debug!("Received NotifyAuxBlock for round {}", round);
                    let block = {
                        let guard = self.inner.blocks.read();
                        guard.get(&round).expect("block created").clone()
                    };

                    tracing::debug!(
                        "{} sending aux block for round {round} to {}",
                        format_authority_index(self.inner.authority),
                        format_authority_index(self.peer)
                    );
                    if self
                        .connection_sender
                        .send(NetworkMessage::AuxiliaryBlock(block))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                DisseminatorMessage::NotifyAuxCertificate(round) => {
                    tracing::debug!("Received NotifyAuxCertificate for round {}", round);
                    let certificate = {
                        let guard = self.inner.certificates.read();
                        guard.get(&round).expect("certificate created").clone()
                    };

                    tracing::debug!(
                        "{} sending certificate for round {round} to {}",
                        format_authority_index(self.inner.authority),
                        format_authority_index(self.peer)
                    );
                    if self
                        .connection_sender
                        .send(NetworkMessage::AuxiliaryCertificate(certificate))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }
}
