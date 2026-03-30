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
    block_handler::{BlockHandler, CommitHandler, RealBlockHandler},
    block_store::{BlockStore, BlockWriter, OwnBlockData, WAL_ENTRY_BLOCK},
    committee::Committee,
    config::{self, NodePrivateConfig, NodePublicConfig},
    core::{Core, CoreOptions},
    data::Data,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    syncer::Syncer,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
    wal::{open_file_for_wal, walf, WalPosition, WalWriter},
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
) -> Core<RealBlockHandler> {
    let metrics = Metrics::new_for_test(committee.len());
    let wal_file = if let Some(path) = path {
        let wal_path = path.join(format!("{:03}.wal", authority));
        open_file_for_wal(&wal_path).unwrap()
    } else {
        tempfile::tempfile().unwrap()
    };
    let (wal_writer, wal_reader) = walf(wal_file).expect("Failed to open wal");
    let recovered = BlockStore::open(
        authority,
        Arc::new(wal_reader),
        &wal_writer,
        metrics.clone(),
        committee,
    );
    let (block_handler, _tx_sender) = RealBlockHandler::new(
        committee.clone(),
        authority,
        None,
        recovered.block_store.clone(),
        metrics.clone(),
        false,
    );
    let private_config = NodePrivateConfig::new_for_tests(authority);
    Core::open(
        block_handler,
        authority,
        committee.clone(),
        private_config,
        public_config,
        metrics,
        recovered,
        wal_writer,
        CoreOptions::test(),
    )
}

pub fn committee_and_cores(n: usize) -> (Arc<Committee>, Vec<Core<RealBlockHandler>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<RealBlockHandler>>) {
    let public_config = NodePublicConfig::new_for_tests(n);
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core_with_real_handler(authority, &committee, &public_config, path))
        .collect();
    (committee, cores)
}

pub fn committee_and_cores_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (Arc<Committee>, Vec<Core<RealBlockHandler>>) {
    let mut public_config = NodePublicConfig::new_for_tests(n);
    public_config.parameters.rounds_in_epoch = rounds_in_epoch;
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core_with_real_handler(authority, &committee, &public_config, None))
        .collect();
    (committee, cores)
}

#[cfg(feature = "simulator")]
pub fn committee_and_syncers(n: usize) -> (Arc<Committee>, Vec<Syncer<RealBlockHandler>>) {
    let (committee, cores) = committee_and_cores(n);
    (
        committee.clone(),
        cores
            .into_iter()
            .map(|core| {
                let commit_handler = CommitHandler::new(
                    committee.clone(),
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
pub fn simulated_network_syncers(
    n: usize,
) -> (SimulatedNetwork, Vec<NetworkSyncer<RealBlockHandler>>) {
    simulated_network_syncers_with_epoch_duration(
        n,
        config::node_defaults::default_rounds_in_epoch(),
    )
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (SimulatedNetwork, Vec<NetworkSyncer<RealBlockHandler>>) {
    let (committee, cores) = committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let (simulated_network, networks) = SimulatedNetwork::new(&committee);
    let public_config = config::NodePublicConfig::new_for_tests(n);
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            committee.clone(),
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

pub async fn network_syncers(n: usize) -> Vec<NetworkSyncer<RealBlockHandler>> {
    network_syncers_with_epoch_duration(n, config::node_defaults::default_rounds_in_epoch()).await
}

pub async fn network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> Vec<NetworkSyncer<RealBlockHandler>> {
    let (committee, cores) = committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            committee.clone(),
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

pub fn check_commits<H: BlockHandler>(syncers: &[Syncer<H>]) {
    let commits = syncers
        .iter()
        .map(|state| state.commit_handler().committed_leaders());
    let zero_commit: &[BlockReference] = &[];
    let mut max_commit = zero_commit;
    for commit in commits {
        if commit.len() >= max_commit.len() {
            if is_prefix(max_commit, commit) {
                max_commit = commit;
            } else {
                panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
            }
        } else if !is_prefix(commit, max_commit) {
            panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
        }
    }
    eprintln!("Max commit sequence: {max_commit:?}");
}

#[cfg(feature = "simulator")]
pub fn print_stats<H: BlockHandler>(syncers: &[Syncer<H>]) {
    use crate::types::format_authority_index;
    for s in syncers {
        let authority = format_authority_index(s.core().authority());
        let snapshot = s.core().metrics.collect();
        tracing::info!("Validator {authority} metrics:\n{snapshot}");
    }
}

fn is_prefix(short: &[BlockReference], long: &[BlockReference]) -> bool {
    assert!(short.len() <= long.len());
    for (a, b) in short.iter().zip(long.iter().take(short.len())) {
        if a != b {
            return false;
        }
    }
    true
}

pub struct TestBlockWriter {
    block_store: BlockStore,
    wal_writer: WalWriter,
}

impl TestBlockWriter {
    pub fn new(committee: &Committee) -> Self {
        let file = tempfile::tempfile().unwrap();
        let (wal_writer, wal_reader) = walf(file).unwrap();
        let state = BlockStore::open(
            0,
            Arc::new(wal_reader),
            &wal_writer,
            Metrics::new_for_test(0),
            committee,
        );
        let block_store = state.block_store;
        Self {
            block_store,
            wal_writer,
        }
    }

    pub fn add_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        let pos = self
            .wal_writer
            .write(WAL_ENTRY_BLOCK, &bincode::serialize(&block).unwrap())
            .unwrap();
        self.block_store.insert_block(block, pos);
        pos
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) {
        for block in blocks {
            self.add_block(block);
        }
    }

    pub fn into_block_store(self) -> BlockStore {
        self.block_store
    }

    pub fn block_store(&self) -> BlockStore {
        self.block_store.clone()
    }
}

impl BlockWriter for TestBlockWriter {
    fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        (&mut self.wal_writer, &self.block_store).insert_block(block)
    }

    fn insert_own_block(&mut self, block: &OwnBlockData) {
        (&mut self.wal_writer, &self.block_store).insert_own_block(block)
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
