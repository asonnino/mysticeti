// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use ::prometheus::Registry;

use dag::{authority::Authority, committee::Committee};

use crate::{
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    replica::Replica,
};

pub struct ReplicaBuilder {
    authority: Authority,
    committee: Arc<Committee>,
    public_config: PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    registry: Registry,
    metrics_server_address: Option<SocketAddr>,
    load_generator_config: Option<LoadGeneratorConfig>,
}

impl ReplicaBuilder {
    pub fn new(
        authority: Authority,
        committee: Arc<Committee>,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> Self {
        Self {
            authority,
            committee,
            public_config,
            private_config,
            registry: Registry::new(),
            metrics_server_address: None,
            load_generator_config: None,
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

    pub fn with_load_generator(mut self, config: LoadGeneratorConfig) -> Self {
        self.load_generator_config = Some(config);
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
            self.load_generator_config,
        )
    }
}
