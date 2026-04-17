// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use consensus::{committer::Committer, protocol::Protocol};
use dag::{
    authority::Authority,
    config::NodePublicConfig,
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    crypto::CryptoEngine,
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
};

use dag::committee::Committee;

use crate::context::SimulatorContext;

type Syncer = NetworkSyncer<SimulatorContext, Committer>;

pub(crate) struct SimulatedReplica {
    authority: Authority,
    committee: Arc<Committee>,
    public_config: NodePublicConfig,
    network: Network,
    commit_period: u64,
}

impl SimulatedReplica {
    pub fn new(
        authority: Authority,
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

    #[tracing::instrument(skip_all, fields(authority = %self.authority))]
    pub fn start(self) -> Syncer {
        let metrics = Metrics::new_for_test(self.committee.len());

        let (storage, recovered) =
            Storage::new_for_tests(self.authority, metrics.clone(), &self.committee);

        let committer = Committer::new(
            self.committee.clone(),
            storage.block_reader().clone(),
            Protocol::mysticeti(
                self.committee.total_stake(),
                self.public_config.parameters.leader_count,
            ),
            metrics.clone(),
        );

        let (block_handler, _) = RealBlockHandler::new(metrics.clone());
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let crypto = CryptoEngine::disabled();
        let core = Core::open(
            block_handler,
            self.authority,
            self.committee,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::test(),
            committer,
            crypto,
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
