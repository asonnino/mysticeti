// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use axum::{Router, extract::State, http::StatusCode, routing::get};
use prometheus::{Registry, TextEncoder};
use tokio::net::TcpListener;

use tokio::{runtime::Handle, task::JoinHandle};

pub const METRICS_ROUTE: &str = "/metrics";

pub fn start_prometheus_server(address: SocketAddr, registry: &Registry) -> JoinHandle<()> {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .with_state(registry.clone());

    Handle::current().spawn(async move {
        let listener = TcpListener::bind(&address)
            .await
            .expect("Failed to bind metrics server");
        tracing::info!("Prometheus server started on {address}");
        axum::serve(listener, app)
            .await
            .expect("Prometheus server failed");
    })
}

async fn metrics(State(registry): State<Registry>) -> (StatusCode, String) {
    let families = registry.gather();
    match TextEncoder.encode_to_string(&families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unable to encode metrics: {error}"),
        ),
    }
}
