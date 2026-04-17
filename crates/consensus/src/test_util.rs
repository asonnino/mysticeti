// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc};

use dag::{
    committee::Committee,
    config::{NodePrivateConfig, NodePublicConfig},
    context::Ctx,
    core::{Core, CoreOptions, block_handler::RealBlockHandler},
    metrics::Metrics,
    storage::Storage,
    test_util::committee,
    types::Authority,
};

use crate::{committer::Committer, protocol::Protocol};

fn open_core<C: Ctx>(
    authority: Authority,
    committee: &Arc<Committee>,
    public_config: &NodePublicConfig,
    path: Option<&Path>,
) -> Core<C, Committer> {
    let metrics = Metrics::new_for_test(committee.len());
    let (storage, recovered) = if let Some(path) = path {
        let wal_path = path.join(format!("{:03}.wal", authority));
        Storage::open(authority, &wal_path, metrics.clone(), committee)
            .expect("Failed to open storage")
    } else {
        Storage::new_for_tests(authority, metrics.clone(), committee)
    };
    let committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol::mysticeti(
            committee.total_stake(),
            public_config.parameters.leader_count,
        ),
        metrics.clone(),
    );
    let (block_handler, _tx_sender) = RealBlockHandler::new(metrics.clone());
    let private_config = NodePrivateConfig::new_for_tests(authority);
    Core::open(
        block_handler,
        authority,
        committee.clone(),
        private_config,
        metrics,
        storage,
        recovered,
        CoreOptions::test(),
        committer,
    )
}

pub fn committee_and_cores<C: Ctx>(n: usize) -> (Arc<Committee>, Vec<Core<C, Committer>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted<C: Ctx>(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<C, Committer>>) {
    let public_config = NodePublicConfig::new_for_tests(n);
    committee_and_cores_with_config(n, path, &public_config)
}

fn committee_and_cores_with_config<C: Ctx>(
    n: usize,
    path: Option<&Path>,
    public_config: &NodePublicConfig,
) -> (Arc<Committee>, Vec<Core<C, Committer>>) {
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core(authority, &committee, public_config, path))
        .collect();
    (committee, cores)
}
