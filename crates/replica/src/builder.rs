// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use ::prometheus::Registry;

use dag::{
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
};

use crate::replica::Replica;

pub struct ReplicaBuilder {
    authority: AuthorityIndex,
    committee: Arc<Committee>,
    public_config: NodePublicConfig,
    private_config: NodePrivateConfig,
    registry: Registry,
    metrics_server_address: Option<SocketAddr>,
    client_parameters: Option<ClientParameters>,
}

impl ReplicaBuilder {
    pub fn new(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
    ) -> Self {
        Self {
            authority,
            committee,
            public_config,
            private_config,
            registry: Registry::new(),
            metrics_server_address: None,
            client_parameters: None,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_metrics_server(mut self, address: SocketAddr) -> Self {
        self.metrics_server_address = Some(address);
        self
    }

    pub fn with_load_generator(mut self, client_parameters: ClientParameters) -> Self {
        self.client_parameters = Some(client_parameters);
        self
    }

    pub fn build(self) -> Replica {
        Replica::new(
            self.authority,
            self.committee,
            self.public_config,
            self.private_config,
            self.registry,
            self.metrics_server_address,
            self.client_parameters,
        )
    }
}
