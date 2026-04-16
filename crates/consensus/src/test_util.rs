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
    types::AuthorityIndex,
};

use crate::protocols::mysticeti::Mysticeti;

fn open_core<C: Ctx>(
    authority: AuthorityIndex,
    committee: &Arc<Committee>,
    public_config: &NodePublicConfig,
    path: Option<&Path>,
) -> Core<C, Mysticeti> {
    let metrics = Metrics::new_for_test(committee.len());
    let (storage, recovered) = if let Some(path) = path {
        let wal_path = path.join(format!("{:03}.wal", authority));
        Storage::open(authority, &wal_path, metrics.clone(), committee)
            .expect("Failed to open storage")
    } else {
        Storage::new_for_tests(authority, metrics.clone(), committee)
    };
    let committer = Mysticeti::new(
        committee.clone(),
        storage.block_reader().clone(),
        metrics.clone(),
        public_config.parameters.number_of_leaders,
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

pub fn committee_and_cores<C: Ctx>(n: usize) -> (Arc<Committee>, Vec<Core<C, Mysticeti>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted<C: Ctx>(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<C, Mysticeti>>) {
    let public_config = NodePublicConfig::new_for_tests(n);
    committee_and_cores_with_config(n, path, &public_config)
}

fn committee_and_cores_with_config<C: Ctx>(
    n: usize,
    path: Option<&Path>,
    public_config: &NodePublicConfig,
) -> (Arc<Committee>, Vec<Core<C, Mysticeti>>) {
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core(authority, &committee, public_config, path))
        .collect();
    (committee, cores)
}
