// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod client;
pub mod server;
mod worker;

#[cfg(test)]
mod tests {

    use std::net::{SocketAddr, TcpListener};

    use futures::join;
    use tokio::sync::mpsc::{self};

    use crate::aux_networking::{client::NetworkClient, server::NetworkServer};

    /// Return a socket address on the local machine with a random port.
    /// This is useful for tests.
    pub fn get_test_address() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind to a random port")
            .local_addr()
            .expect("Failed to get local address")
    }

    #[tokio::test]
    async fn client_primary_connection() {
        let server_address = get_test_address();
        let transactions: Vec<_> = (0..100).collect();

        // Spawn the server, wait for a client connection, and receive transactions from the client.
        let cloned_transactions = transactions.clone();
        let server = async move {
            let (tx_client_connections, mut rx_client_connections) = mpsc::channel(1);
            let server = NetworkServer::<_, ()>::new(server_address, tx_client_connections);
            let _server_handle = server.spawn();

            // Wait for a client connection and hold it (to avoid closing the channel).
            let mut connection = rx_client_connections.recv().await.unwrap();

            // Wait for the result.
            for i in cloned_transactions {
                let t: u32 = connection.recv().await.unwrap();
                assert_eq!(t, i);
            }
        };

        // Spawn the client and send transactions to the primary.
        let client = async move {
            let (tx_unused, _rx_unused) = mpsc::channel(1);
            let (tx_transactions, rx_transactions) = mpsc::channel(1);

            let client = NetworkClient::<(), _>::new(server_address, tx_unused, rx_transactions);
            let _client_handle = client.spawn();

            // Send a transaction to the primary.
            for t in transactions {
                tx_transactions.send(t).await.unwrap();
            }
        };

        // Ensure both the client and server completed successfully.
        join!(server, client);
    }

    #[tokio::test]
    async fn proxy_primary_connection() {
        let server_address = get_test_address();
        let transaction = "transaction".to_string();
        let result = "transaction result".to_string();

        // Spawn the server, wait for a proxy connection, and send a transaction to the proxy.
        let cloned_transaction = transaction.clone();
        let cloned_result = result.clone();
        let server = async move {
            let (tx_proxy_connections, mut rx_proxy_connections) = mpsc::channel(1);

            let server = NetworkServer::new(server_address, tx_proxy_connections);
            let _server_handle = server.spawn();

            // Wait for a proxy connection and send a transaction.
            let mut connection = rx_proxy_connections.recv().await.unwrap();
            connection.send(cloned_transaction).await.unwrap();

            // Wait for the result.
            let r: String = connection.recv().await.unwrap();
            assert_eq!(r, cloned_result);
        };

        // Spawn the client, wait for a transaction from the primary, and send back a result.
        let client = async move {
            let (tx_transactions, mut rx_transactions) = mpsc::channel(1);
            let (tx_proxy_results, rx_proxy_results) = mpsc::channel(1);

            let client = NetworkClient::new(server_address, tx_transactions, rx_proxy_results);
            let _client_handle = client.spawn();

            let t: String = rx_transactions.recv().await.unwrap();
            assert_eq!(t, transaction);
            tx_proxy_results.send(result).await.unwrap();
        };

        // Ensure both the client and server completed successfully.
        join!(server, client);
    }
}
