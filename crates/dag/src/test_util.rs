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
                .map(Block::genesis)
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
                let block = Data::new(Block::new_for_test(authority, round, includes.clone()));
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
        let block = Data::new(Block::new_for_test(authority, round, parents));

        references.push(*block.reference());
        storage.insert_block(block);
    }
    references
}

/// Insert a hand-built test block and return its reference. Use this for
/// blocks the layer builders cannot express, such as an equivocating block
/// built with [`Block::with_digest`].
pub fn insert_test_block(block: Block, storage: &mut Storage) -> BlockReference {
    let block = Data::new(block);
    let reference = *block.reference();
    storage.insert_block(block);
    reference
}

/// Return the references at a round excluding the given leader's block.
/// Convenience for tests that need to drive a "leader missed its proposal"
/// scenario over an existing layer of refs.
pub fn drop_leader(refs: &[BlockReference], leader: Authority) -> Vec<BlockReference> {
    refs.iter()
        .copied()
        .filter(|reference| reference.authority != leader)
        .collect()
}

/// Build a "split chain" DAG: starting from `support` (refs whose causal past
/// includes some target leader L) and `blames` (refs that exclude L), advance
/// the two chains in lockstep up to `target_round`. At each intermediate round,
/// authority 0 carries the support chain (parents = previous support refs) and
/// authorities `1..n` carry the blame chain (parents = previous blame refs). At
/// the final `target_round`, the layer is split into `n - final_blamers_count`
/// support blocks and `final_blamers_count` blame blocks.
///
/// Pre-conditions: `support` and `blames` must be at the same round R,
/// `target_round` must be `>= R + 1`, and `final_blamers_count` must be `< n`.
pub fn build_split_chain(
    committee: &Committee,
    storage: &mut Storage,
    support: Vec<BlockReference>,
    blames: Vec<BlockReference>,
    target_round: RoundNumber,
    final_blamers_count: usize,
) -> (Vec<BlockReference>, Vec<BlockReference>) {
    let n = committee.len();
    let initial_round = support.first().unwrap().round;
    assert_eq!(
        initial_round,
        blames.first().unwrap().round,
        "support and blames must be at the same round",
    );
    assert!(target_round > initial_round);
    assert!(final_blamers_count < n);

    let mut support = support;
    let mut blames = blames;
    let mut current_round = initial_round;

    while current_round + 1 < target_round {
        let next_support = build_dag_layer(vec![(Authority::from(0u64), support.clone())], storage);
        let next_blames = build_dag_layer(
            (1..n as u64)
                .map(|i| (Authority::from(i), blames.clone()))
                .collect(),
            storage,
        );
        support = next_support;
        blames = next_blames;
        current_round += 1;
    }

    let supporters_count = n - final_blamers_count;
    let final_support = build_dag_layer(
        (0..supporters_count as u64)
            .map(|i| (Authority::from(i), support.clone()))
            .collect(),
        storage,
    );
    let final_blames = build_dag_layer(
        (supporters_count as u64..n as u64)
            .map(|i| (Authority::from(i), blames.clone()))
            .collect(),
        storage,
    );
    (final_support, final_blames)
}
