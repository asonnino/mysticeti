// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use consensus::committer::Committer;
use dag::{
    authority::Authority,
    core::{
        Core,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    crypto::CryptoEngine,
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
};

use dag::committee::Committee;

use crate::{context::SimulatorContext, params::ReplicaParameters};

type Syncer = NetworkSyncer<SimulatorContext, Committer>;

pub(crate) struct SimulatedReplica {
    authority: Authority,
    committee: Arc<Committee>,
    parameters: ReplicaParameters,
    network: Network,
}

impl SimulatedReplica {
    pub fn new(
        authority: Authority,
        committee: Arc<Committee>,
        parameters: ReplicaParameters,
        network: Network,
    ) -> Self {
        Self {
            authority,
            committee,
            parameters,
            network,
        }
    }

    #[tracing::instrument(skip_all, fields(authority = %self.authority))]
    pub fn start(self) -> Syncer {
        let metrics = Metrics::new_for_test(self.committee.len());

        let (storage, recovered) =
            Storage::new_for_tests(self.authority, metrics.clone(), &self.committee);

        let commit_period = self.parameters.consensus.wave_length();
        let protocol = self
            .parameters
            .consensus
            .to_protocol(self.committee.total_stake());
        let round_timeout = self
            .parameters
            .dag
            .round_timeout
            .unwrap_or_else(|| protocol.default_round_timeout());
        let committer = Committer::new(
            self.committee.clone(),
            storage.block_reader().clone(),
            protocol,
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
            self.parameters.dag.fsync,
            committer,
            crypto,
        );

        NetworkSyncer::start(
            self.network,
            core,
            commit_period,
            round_timeout,
            self.parameters.dag.enable_synchronizer,
            commit_handler,
            metrics,
        )
    }
}
