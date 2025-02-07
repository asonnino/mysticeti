// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, net::SocketAddr};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::certifier_network::worker::ConnectionWorker;

/// The size of the server-worker communication channel.
pub const WORKER_CHANNEL_SIZE: usize = 1000;

pub struct Connection<I, O> {
    /// The address of the peer connected to the server.
    pub peer: SocketAddr,
    /// The sender for the messages to send through the network.
    pub tx_outgoing: Sender<O>,
    /// The receiver for the messages received from the network.
    pub rx_incoming: Receiver<I>,
}

impl<I, O> Connection<I, O> {
    /// Receive a message from the network.
    pub async fn recv(&mut self) -> Option<I> {
        self.rx_incoming.recv().await
    }

    /// Send a message through the network.
    pub async fn send(&self, message: O) -> io::Result<()> {
        self.tx_outgoing
            .send(message)
            .await
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    pub fn split(self) -> (Sender<O>, Receiver<I>) {
        (self.tx_outgoing, self.rx_incoming)
    }
}

/// A server run by the primary machine that listens for connections from proxies
/// and transactions from clients. When a new client connects, a new worker is spawned.
/// The server can leverage the bidirectional channel opened by the client to send messages
/// as well. To this purpose, the server propagate a communication channel to the application
/// layer, which can use it to send messages to the client.
pub struct NetworkServer<I, O> {
    id: usize,
    /// The socket address of the server.
    server_address: SocketAddr,
    /// The sender for client connections. When a new client connects, this channel
    /// is used to propagate a (tx, rx) handles to the application layer, which can use it
    /// to send and messages from the client.
    tx_connections: Sender<Connection<I, O>>,
}

impl<I, O> NetworkServer<I, O>
where
    I: Send + DeserializeOwned + 'static,
    O: Send + Serialize + 'static,
{
    /// Create a new server.
    pub fn new(
        id: usize,
        server_address: SocketAddr,
        tx_connections: Sender<Connection<I, O>>,
    ) -> Self {
        Self {
            id,
            server_address,
            tx_connections,
        }
    }

    /// Run the server.
    pub async fn run(&self) -> io::Result<()> {
        let id = self.id;
        let server = TcpListener::bind(self.server_address).await?;
        tracing::info!("[{id}] Listening on {}", self.server_address);

        loop {
            let (stream, peer) = server.accept().await?;
            stream.set_nodelay(true)?;
            tracing::info!("[{id}] Accepted connection from client {peer}");

            // Spawn a worker to handle the connection.
            let (tx_incoming, rx_incoming) = mpsc::channel(WORKER_CHANNEL_SIZE);
            let (tx_outgoing, rx_outgoing) = mpsc::channel(WORKER_CHANNEL_SIZE);
            let worker = ConnectionWorker::new(id, stream, tx_incoming, rx_outgoing);
            let _handle = worker.spawn();

            // Notify the application layer that a new client has connected.
            let connection = Connection {
                peer,
                tx_outgoing,
                rx_incoming,
            };
            let result = self.tx_connections.send(connection).await;
            if result.is_err() {
                tracing::warn!("[{id}] Cannot send connection to application, stopping server");
                break Ok(());
            }
        }
    }

    /// Spawn the server in a new task.
    pub fn spawn(self) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move { self.run().await })
    }
}
