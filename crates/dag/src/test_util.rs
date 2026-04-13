// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
    sync::Arc,
};

use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};

use crate::{
    committee::Committee,
    config::{NodePrivateConfig, NodePublicConfig},
    context::{Ctx, TokioCtx},
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
        syncer::Syncer,
    },
    data::Data,
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

pub fn committee(n: usize) -> Arc<Committee> {
    Committee::new_test(vec![1; n])
}

fn open_core<C: Ctx>(
    authority: AuthorityIndex,
    committee: &Arc<Committee>,
    public_config: &NodePublicConfig,
    path: Option<&Path>,
) -> Core<C> {
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

pub fn committee_and_cores<C: Ctx>(n: usize) -> (Arc<Committee>, Vec<Core<C>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted<C: Ctx>(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<C>>) {
    let public_config = NodePublicConfig::new_for_tests(n);
    committee_and_cores_with_config(n, path, &public_config)
}

fn committee_and_cores_with_config<C: Ctx>(
    n: usize,
    path: Option<&Path>,
    public_config: &NodePublicConfig,
) -> (Arc<Committee>, Vec<Core<C>>) {
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core(authority, &committee, public_config, path))
        .collect();
    (committee, cores)
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

pub async fn network_syncers(n: usize) -> Vec<NetworkSyncer<TokioCtx>> {
    let (_committee, cores) = committee_and_cores::<TokioCtx>(n);
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
            Metrics::new_for_test(0),
            &NodePublicConfig::new_for_tests(n),
        );
        network_syncers.push(network_syncer);
    }
    network_syncers
}

pub fn rng_at_seed(seed: u64) -> StdRng {
    let bytes = seed.to_le_bytes();
    let mut seed = [0u8; 32];
    seed[..bytes.len()].copy_from_slice(&bytes);
    StdRng::from_seed(seed)
}

pub fn check_commits<C: Ctx>(syncers: &[Syncer<C>]) {
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

pub fn print_stats<C: Ctx>(syncers: &[Syncer<C>]) {
    use crate::types::format_authority_index;
    for s in syncers {
        let authority = format_authority_index(s.core().authority());
        let snapshot = s.core().metrics.collect();
        tracing::info!("Validator {authority} metrics:\n{snapshot}");
    }
}

pub fn build_dag(
    committee: &Committee,
    storage: &mut Storage,
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
            for block in genesis {
                storage.insert_block(block);
            }
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
                    Default::default(),
                ));
                (*block.reference(), block)
            })
            .unzip();
        for block in blocks {
            storage.insert_block(block);
        }
        includes = references;
    }

    includes
}

pub fn build_dag_layer(
    connections: Vec<(AuthorityIndex, Vec<BlockReference>)>,
    storage: &mut Storage,
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
            Default::default(),
        ));

        references.push(*block.reference());
        storage.insert_block(block);
    }
    references
}
