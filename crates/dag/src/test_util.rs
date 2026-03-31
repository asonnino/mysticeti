// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
    sync::Arc,
};

use futures::future::join_all;
#[cfg(feature = "simulator")]
use rand::{rngs::StdRng, SeedableRng};

#[cfg(feature = "simulator")]
use crate::future_simulator::OverrideNodeContext;
#[cfg(feature = "simulator")]
use crate::simulated_network::SimulatedNetwork;
#[cfg(feature = "simulator")]
use crate::syncer::SyncerSignals;
use crate::{
    block_handler::{CommitHandler, RealBlockHandler},
    committee::Committee,
    config::{self, NodePrivateConfig, NodePublicConfig},
    context::DefaultCtx,
    core::{Core, CoreOptions},
    data::Data,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    storage::BlockReader,
    storage::Storage,
    syncer::Syncer,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
    wal::WalPosition,
};

pub fn committee(n: usize) -> Arc<Committee> {
    Committee::new_test(vec![1; n])
}

// --- Core construction helpers ---

fn open_core_with_real_handler(
    authority: AuthorityIndex,
    committee: &Arc<Committee>,
    public_config: &NodePublicConfig,
    path: Option<&Path>,
) -> Core<DefaultCtx> {
    let metrics = Metrics::new_for_test(committee.len());
    let (storage, recovered) = if let Some(path) = path {
        let wal_path = path.join(format!("{:03}.wal", authority));
        Storage::open(authority, &wal_path, metrics.clone(), committee)
            .expect("Failed to open storage")
    } else {
        Storage::new_for_tests(authority, metrics.clone(), committee)
    };
    let (block_handler, _tx_sender) = RealBlockHandler::new(metrics.clone());
    let private_config = NodePrivateConfig::new_for_tests(authority);
    Core::open(
        block_handler,
        authority,
        committee.clone(),
        private_config,
        public_config,
        metrics,
        storage,
        recovered,
        CoreOptions::test(),
    )
}

pub fn committee_and_cores(n: usize) -> (Arc<Committee>, Vec<Core<DefaultCtx>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<DefaultCtx>>) {
    let public_config = NodePublicConfig::new_for_tests(n);
    committee_and_cores_with_config(n, path, &public_config)
}

pub fn committee_and_cores_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (Arc<Committee>, Vec<Core<DefaultCtx>>) {
    let mut public_config = NodePublicConfig::new_for_tests(n);
    public_config.parameters.rounds_in_epoch = rounds_in_epoch;
    committee_and_cores_with_config(n, None, &public_config)
}

fn committee_and_cores_with_config(
    n: usize,
    path: Option<&Path>,
    public_config: &NodePublicConfig,
) -> (Arc<Committee>, Vec<Core<DefaultCtx>>) {
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core_with_real_handler(authority, &committee, public_config, path))
        .collect();
    (committee, cores)
}

#[cfg(feature = "simulator")]
pub fn committee_and_syncers(n: usize) -> (Arc<Committee>, Vec<Syncer<DefaultCtx>>) {
    let (committee, cores) = committee_and_cores(n);
    (
        committee.clone(),
        cores
            .into_iter()
            .map(|core| {
                let commit_handler = CommitHandler::new(
                    core.block_handler().transaction_time.clone(),
                    Metrics::new_for_test(0),
                );
                Syncer::new(
                    core,
                    3,
                    SyncerSignals::test(),
                    commit_handler,
                    Metrics::new_for_test(0),
                )
            })
            .collect(),
    )
}

pub async fn networks_and_addresses(metrics: &[Arc<Metrics>]) -> (Vec<Network>, Vec<SocketAddr>) {
    let host = Ipv4Addr::LOCALHOST;
    let addresses: Vec<_> = (0..metrics.len())
        .map(|i| SocketAddr::V4(SocketAddrV4::new(host, 5001 + i as u16)))
        .collect();
    let networks =
        addresses
            .iter()
            .zip(metrics.iter())
            .enumerate()
            .map(|(i, (address, metrics))| {
                Network::from_socket_addresses(&addresses, i, *address, metrics.clone())
            });
    let networks = join_all(networks).await;
    (networks, addresses)
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers(n: usize) -> (SimulatedNetwork, Vec<NetworkSyncer<DefaultCtx>>) {
    simulated_network_syncers_with_epoch_duration(
        n,
        config::node_defaults::default_rounds_in_epoch(),
    )
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (SimulatedNetwork, Vec<NetworkSyncer<DefaultCtx>>) {
    let (committee, cores) = committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let (simulated_network, networks) = SimulatedNetwork::new(&committee);
    let public_config = config::NodePublicConfig::new_for_tests(n);
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            core.metrics.clone(),
        );
        let node_context = OverrideNodeContext::enter(Some(core.authority()));
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            3,
            commit_handler,
            config::node_defaults::default_shutdown_grace_period(),
            Metrics::new_for_test(0),
            &public_config,
        );
        drop(node_context);
        network_syncers.push(network_syncer);
    }
    (simulated_network, network_syncers)
}

pub async fn network_syncers(n: usize) -> Vec<NetworkSyncer<DefaultCtx>> {
    network_syncers_with_epoch_duration(n, config::node_defaults::default_rounds_in_epoch()).await
}

pub async fn network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> Vec<NetworkSyncer<DefaultCtx>> {
    let (_committee, cores) = committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            3,
            commit_handler,
            config::node_defaults::default_shutdown_grace_period(),
            Metrics::new_for_test(0),
            &NodePublicConfig::new_for_tests(n),
        );
        network_syncers.push(network_syncer);
    }
    network_syncers
}

#[cfg(feature = "simulator")]
pub fn rng_at_seed(seed: u64) -> StdRng {
    let bytes = seed.to_le_bytes();
    let mut seed = [0u8; 32];
    seed[..bytes.len()].copy_from_slice(&bytes);
    StdRng::from_seed(seed)
}

pub fn check_commits(syncers: &[Syncer<DefaultCtx>]) {
    let commits = syncers
        .iter()
        .map(|state| state.commit_handler().committed_leaders());
    let zero_commit: &[BlockReference] = &[];
    let mut max_commit = zero_commit;
    for commit in commits {
        if commit.len() >= max_commit.len() {
            if commit.starts_with(max_commit) {
                max_commit = commit;
            } else {
                panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
            }
        } else if !max_commit.starts_with(commit) {
            panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
        }
    }
    eprintln!("Max commit sequence: {max_commit:?}");
}

#[cfg(feature = "simulator")]
pub fn print_stats(syncers: &[Syncer<DefaultCtx>]) {
    use crate::types::format_authority_index;
    for s in syncers {
        let authority = format_authority_index(s.core().authority());
        let snapshot = s.core().metrics.collect();
        tracing::info!("Validator {authority} metrics:\n{snapshot}");
    }
}

pub struct TestBlockWriter {
    storage: Storage,
}

impl TestBlockWriter {
    pub fn new(committee: &Committee) -> Self {
        let (storage, _recovered) = Storage::new_for_tests(0, Metrics::new_for_test(0), committee);
        Self { storage }
    }

    pub fn add_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        self.storage.insert_block(block)
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) {
        for block in blocks {
            self.add_block(block);
        }
    }

    pub fn into_block_reader(self) -> BlockReader {
        self.storage.block_reader().clone()
    }
}

/// Build a fully interconnected dag up to the specified round. This function starts building the
/// dag from the specified [`start`] references or from genesis if none are specified.
pub fn build_dag(
    committee: &Committee,
    block_writer: &mut TestBlockWriter,
    start: Option<Vec<BlockReference>>,
    stop: RoundNumber,
) -> Vec<BlockReference> {
    let mut includes = match start {
        Some(start) => {
            assert!(!start.is_empty());
            assert_eq!(
                start.iter().map(|x| x.round).max(),
                start.iter().map(|x| x.round).min()
            );
            start
        }
        None => {
            let (references, genesis): (Vec<_>, Vec<_>) = committee
                .authorities()
                .map(StatementBlock::new_genesis)
                .map(|block| (*block.reference(), block))
                .unzip();
            block_writer.add_blocks(genesis);
            references
        }
    };

    let starting_round = includes.first().unwrap().round + 1;
    for round in starting_round..=stop {
        let (references, blocks): (Vec<_>, Vec<_>) = committee
            .authorities()
            .map(|authority| {
                let block = Data::new(StatementBlock::new(
                    authority,
                    round,
                    includes.clone(),
                    vec![],
                    0,
                    false,
                    Default::default(),
                ));
                (*block.reference(), block)
            })
            .unzip();
        block_writer.add_blocks(blocks);
        includes = references;
    }

    includes
}

pub fn build_dag_layer(
    // A list of (authority, parents) pairs. For each authority, we add a block linking to the
    // specified parents.
    connections: Vec<(AuthorityIndex, Vec<BlockReference>)>,
    block_writer: &mut TestBlockWriter,
) -> Vec<BlockReference> {
    let mut references = Vec::new();
    for (authority, parents) in connections {
        let round = parents.first().unwrap().round + 1;
        let block = Data::new(StatementBlock::new(
            authority,
            round,
            parents,
            vec![],
            0,
            false,
            Default::default(),
        ));

        references.push(*block.reference());
        block_writer.add_block(block);
    }
    references
}
