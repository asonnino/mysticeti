// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ::prometheus::{Registry, TextEncoder};
use axum::{Router, extract::State, http::StatusCode, routing::get};
use eyre::{Context, Result};
use tokio::{net::TcpListener, runtime::Handle, task::JoinHandle};

pub use ::prometheus::Registry as MetricsRegistry;

pub const METRICS_ROUTE: &str = "/metrics";

/// Tokio-backed Prometheus metrics HTTP server.
pub struct PrometheusServer<'a> {
    address: SocketAddr,
    registry: &'a Registry,
}

impl<'a> PrometheusServer<'a> {
    pub fn new(address: SocketAddr, registry: &'a Registry) -> Self {
        Self { address, registry }
    }

    /// Replace the bind IP with `0.0.0.0` so the server is reachable from any network interface
    /// (including remote hosts). Keeps the port unchanged.
    pub fn bind_all_interfaces(mut self) -> Self {
        self.address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        self
    }

    /// Bind the listener and spawn the server on the current tokio runtime. Bind failures
    /// (port in use, permission denied, etc.) surface here; a crash inside the serve loop is
    /// unrecoverable and propagates via the returned join handle.
    pub async fn start(self) -> Result<JoinHandle<()>> {
        let listener = TcpListener::bind(&self.address)
            .await
            .wrap_err_with(|| format!("failed to bind metrics server at {}", self.address))?;
        let app = Router::new()
            .route(METRICS_ROUTE, get(metrics))
            .with_state(self.registry.clone());
        Ok(Handle::current().spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Prometheus server failed");
        }))
    }
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
