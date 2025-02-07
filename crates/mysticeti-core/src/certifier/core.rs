// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use hyper::client::connect;
use tokio::{
    sync::mpsc::{channel, Receiver},
    task::JoinHandle,
};

use super::voter::Voter;
use crate::{
    certifier::{
        aggregator::{self, AggregatorCollection},
        CertifierMessage,
    },
    certifier_network::{
        client::NetworkClient,
        server::{Connection, NetworkServer},
    },
    committee::Committee,
    crypto::Signer,
    network::Network,
    types::AuthorityIndex,
};

const DEFAULT_CERTIFIER_CHANNEL_SIZE: usize = 1024;

pub struct CertifierCore {
    voter: Voter,
    // aggregator: AggregatorCollection,
    // server: NetworkServer<CertifierMessage, CertifierMessage>,
    connections: HashMap<AuthorityIndex, Connection<CertifierMessage, CertifierMessage>>,
    // rx_vote: Receiver<CertifierMessage>,
}

impl CertifierCore {
    pub fn new(
        authority: AuthorityIndex,
        signer: Signer,
        committee: Arc<Committee>,
        socket_address: SocketAddr,
    ) -> Self {
        // client : vote and get back certificates
        let (tx_vote, rx_vote) = channel(DEFAULT_CERTIFIER_CHANNEL_SIZE);
        let (tx_certificate, rx_certificate) = channel(DEFAULT_CERTIFIER_CHANNEL_SIZE);

        let committee_size = committee.len();
        let voter = Voter::new(authority, signer, &committee, tx_vote);
        let certificate_handler = CertificateHandler::new(committee.clone());
        certificate_handler.spawn(rx_certificate);
        let client_handle =
            NetworkClient::new(authority as usize, socket_address, tx_certificate, rx_vote).spawn();

        // server : receive votes and send certificates
        todo!()
    }
}

pub struct CertificateHandler {
    committee: Arc<Committee>,
}

impl CertificateHandler {
    pub fn new(committee: Arc<Committee>) -> Self {
        Self { committee }
    }

    pub async fn run(mut self, mut receiver: Receiver<CertifierMessage>) {
        while let Some(message) = receiver.recv().await {
            if let CertifierMessage::Certificate(certificate) = message {
                todo!()
            }
        }
    }

    pub fn spawn(self, receiver: Receiver<CertifierMessage>) -> JoinHandle<()> {
        tokio::spawn(self.run(receiver))
    }
}
