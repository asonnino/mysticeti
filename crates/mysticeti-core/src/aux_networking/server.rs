// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, net::SocketAddr};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};

use crate::aux_networking::worker::ConnectionWorker;

/// The size of the server-worker communication channel.
pub const WORKER_CHANNEL_SIZE: usize = 1000;

/// A server run by the primary machine that listens for connections from proxies
/// and transactions from clients. When a new client connects, a new worker is spawned.
/// The server can leverage the bidirectional channel opened by the client to send messages
/// as well. To this purpose, the server propagate a communication channel to the application
/// layer, which can use it to send messages to the client.
pub struct NetworkServer<I, O> {
    /// The socket address of the server.
    server_address: SocketAddr,
    /// The sender for client connections. When a new client connects, this channel
    /// is used to propagate a sender to the application layer, which can use it
    /// to send messages to the client.
    tx_connections: Sender<Sender<O>>,
    /// The sender for messages received from the network to send to the application layer.
    tx_incoming: Sender<I>,
}

impl<I, O> NetworkServer<I, O>
where
    I: Send + DeserializeOwned + 'static,
    O: Send + Serialize + 'static,
{
    /// Create a new server.
    pub fn new(
        server_address: SocketAddr,
        tx_connections: Sender<Sender<O>>,
        tx_incoming: Sender<I>,
    ) -> Self {
        Self {
            server_address,
            tx_connections,
            tx_incoming,
        }
    }

    /// Run the server.
    pub async fn run(&self) -> io::Result<()> {
        let server = TcpListener::bind(self.server_address).await?;
        tracing::info!("Listening on {}", self.server_address);

        loop {
            let (stream, peer) = server.accept().await?;
            stream.set_nodelay(true)?;
            tracing::info!("Accepted connection from client {peer}");

            // Spawn a worker to handle the connection.
            let (tx, rx) = mpsc::channel(WORKER_CHANNEL_SIZE);
            let worker = ConnectionWorker::new(stream, self.tx_incoming.clone(), rx);
            let _handle = worker.spawn();

            // Notify the application layer that a new client has connected.
            let result = self.tx_connections.send(tx).await;
            if result.is_err() {
                tracing::warn!("Cannot send connection to application, stopping server");
                break Ok(());
            }
        }
    }

    /// Spawn the server in a new task.
    pub fn spawn(self) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move { self.run().await })
    }
}
