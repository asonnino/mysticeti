// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ::prometheus::Registry;

use dag::authority::Authority;

use crate::{
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    replica::Replica,
};

pub struct ReplicaBuilder {
    authority: Authority,
    public_config: PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    registry: Registry,
    metrics_server_enabled: bool,
    load_generator_config: Option<LoadGeneratorConfig>,
}

impl ReplicaBuilder {
    pub fn new(
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> Self {
        Self {
            authority,
            public_config,
            private_config,
            registry: Registry::new(),
            metrics_server_enabled: false,
            load_generator_config: None,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// Enable the Prometheus metrics HTTP server. The replica binds it
    /// to this authority's `metrics_address` (from the public config),
    /// rebound to 0.0.0.0 so it is reachable from outside.
    pub fn with_metrics_server(mut self) -> Self {
        self.metrics_server_enabled = true;
        self
    }

    pub fn with_load_generator(mut self, config: LoadGeneratorConfig) -> Self {
        self.load_generator_config = Some(config);
        self
    }

    pub fn build(self) -> Replica {
        Replica::new(
            self.authority,
            self.public_config,
            self.private_config,
            self.registry,
            self.metrics_server_enabled,
            self.load_generator_config,
        )
    }
}
