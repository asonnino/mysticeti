// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};

use crate::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Committee,
    consensus::DagConsensus,
    context::Ctx,
    core::syncer::Syncer,
    data::Data,
    metrics::Metrics,
    storage::Storage,
    sync::network::Network,
};

pub fn committee(n: usize) -> Arc<Committee> {
    Committee::new_test(vec![1; n])
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

pub fn rng_at_seed(seed: u64) -> StdRng {
    let bytes = seed.to_le_bytes();
    let mut seed = [0u8; 32];
    seed[..bytes.len()].copy_from_slice(&bytes);
    StdRng::from_seed(seed)
}

pub fn check_commits<C: Ctx, D: DagConsensus>(syncers: &[Syncer<C, D>]) {
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

pub fn print_stats<C: Ctx, D: DagConsensus>(syncers: &[Syncer<C, D>]) {
    for s in syncers {
        let authority = s.core().authority();
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
                .map(Block::new_genesis)
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
                let block = Data::new(Block::new(
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
    connections: Vec<(Authority, Vec<BlockReference>)>,
    storage: &mut Storage,
) -> Vec<BlockReference> {
    let mut references = Vec::new();
    for (authority, parents) in connections {
        let round = parents.first().unwrap().round + 1;
        let block = Data::new(Block::new(
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
