// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use consensus::protocols::mysticeti::Mysticeti;
use dag::{
    config::{NodePrivateConfig, NodePublicConfig},
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
    types::AuthorityIndex,
};

use dag::committee::Committee;

use crate::context::SimulatorContext;

type Syncer = NetworkSyncer<SimulatorContext, Mysticeti>;

pub(crate) struct SimulatedReplica {
    authority: AuthorityIndex,
    committee: Arc<Committee>,
    public_config: NodePublicConfig,
    network: Network,
    commit_period: u64,
}

impl SimulatedReplica {
    pub fn new(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        network: Network,
        commit_period: u64,
    ) -> Self {
        Self {
            authority,
            committee,
            public_config,
            network,
            commit_period,
        }
    }

    #[tracing::instrument(skip_all, fields(authority = self.authority))]
    pub fn start(self) -> Syncer {
        let metrics = Metrics::new_for_test(self.committee.len());

        let (storage, recovered) =
            Storage::new_for_tests(self.authority, metrics.clone(), &self.committee);

        let committer = Mysticeti::new(
            self.committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
            self.public_config.parameters.number_of_leaders,
        );

        let (block_handler, _) = RealBlockHandler::new(metrics.clone());
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let private_config = NodePrivateConfig::new_for_tests(self.authority);
        let core = Core::open(
            block_handler,
            self.authority,
            self.committee,
            private_config,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::test(),
            committer,
        );

        NetworkSyncer::start(
            self.network,
            core,
            self.commit_period,
            commit_handler,
            metrics,
            &self.public_config,
        )
    }
}
