// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use ::prometheus::Registry;
use dag::{authority::Authority, metrics::Metrics, sync::network::Network};

use crate::{
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::Replica,
};

/// How the replica's storage should be constructed.
pub enum StorageKind {
    /// Durable WAL-backed storage at the given path.
    Wal(PathBuf),
    /// Temporary persistent store. Intended for simulation and tests.
    Ephemeral,
}

/// Assembles a [`Replica`]. The caller picks the execution context
/// later via [`Replica::run`]; the builder itself is context-agnostic.
pub struct ReplicaBuilder {
    authority: Authority,
    public_config: PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    storage: StorageKind,
    crypto_disabled: bool,
    metrics: Option<Arc<Metrics>>,
    network: Option<Network>,
    registry: Registry,
}

impl ReplicaBuilder {
    pub fn new(
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> Self {
        let storage = StorageKind::Wal(private_config.wal());
        Self {
            authority,
            public_config,
            private_config,
            storage,
            crypto_disabled: false,
            metrics: None,
            network: None,
            registry: Registry::new(),
        }
    }

    /// Override the default WAL-backed storage.
    pub fn with_storage(mut self, storage: StorageKind) -> Self {
        self.storage = storage;
        self
    }

    /// Force the replica to run without signature verification, even
    /// if the chosen protocol normally requires it. Intended for
    /// simulation runs that want to skip crypto cost.
    pub fn with_crypto_disabled(mut self) -> Self {
        self.crypto_disabled = true;
        self
    }

    /// Inject a pre-built metrics instance. If omitted, the metrics
    /// are constructed during [`Replica::run`] (tokio-backed).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Inject a pre-built network. If omitted, real TCP sockets are
    /// bound during [`Replica::run`] (tokio-only).
    pub fn with_network(mut self, network: Network) -> Self {
        self.network = Some(network);
        self
    }

    /// Override the default Prometheus registry.
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// Finalize configuration. The returned [`Replica`] holds the
    /// same intent; no tokio work has happened yet.
    pub fn build(self) -> Replica {
        Replica {
            authority: self.authority,
            public_config: self.public_config,
            private_config: self.private_config,
            storage: self.storage,
            crypto_disabled: self.crypto_disabled,
            metrics: self.metrics,
            network: self.network,
            registry: self.registry,
        }
    }
}
