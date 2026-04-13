// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use eyre::{Context, Result, eyre};
use prometheus::Registry;

use dag::{
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    context::TokioCtx,
    core::{
        Core, CoreOptions,
        block_handler::{CommitHandler, RealBlockHandler},
    },
    metrics::{self, Metrics},
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
    transactions_generator::TransactionGenerator,
    types::AuthorityIndex,
};
use mysticeti_consensus::universal_committer::{UniversalCommitter, UniversalCommitterBuilder};

pub struct Validator {
    network_synchronizer: NetworkSyncer<TokioCtx, UniversalCommitter>,
    metrics_handle: tokio::task::JoinHandle<()>,
}

impl Validator {
    pub async fn start(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
    ) -> Result<Self> {
        let network_address = public_config
            .network_address(authority)
            .ok_or(eyre!("No network address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_network_address = network_address;
        binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        let metrics_address = public_config
            .metrics_address(authority)
            .ok_or(eyre!("No metrics address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Boot the prometheus server.
        let registry = Registry::new();
        let metrics = Metrics::new(&registry, committee.len(), None);

        let metrics_handle =
            metrics::server::start_prometheus_server(binding_metrics_address, &registry);

        // Open storage.
        let (storage, recovered) =
            Storage::open(authority, private_config.wal(), metrics.clone(), &committee)
                .expect("Failed to open storage");

        // Boot the validator node.
        let (block_handler, block_sender) = RealBlockHandler::new(metrics.clone());

        TransactionGenerator::<TokioCtx>::start(
            block_sender,
            authority,
            client_parameters,
            public_config.clone(),
            metrics.clone(),
        );
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());
        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
        )
        .with_number_of_leaders(public_config.parameters.number_of_leaders)
        .with_pipeline(public_config.parameters.enable_pipelining)
        .build();
        tracing::info!(
            "Pipeline enabled: {}",
            public_config.parameters.enable_pipelining
        );
        tracing::info!(
            "Number of leaders: {}",
            public_config.parameters.number_of_leaders
        );
        let core = Core::open(
            block_handler,
            authority,
            committee.clone(),
            private_config,
            metrics.clone(),
            storage,
            recovered,
            CoreOptions::default(),
            committer,
        );
        let network = Network::load(
            &public_config,
            authority,
            binding_network_address,
            metrics.clone(),
        )
        .await;
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            public_config.parameters.wave_length,
            commit_handler,
            metrics,
            &public_config,
        );

        tracing::info!("Validator {authority} listening on {network_address}");
        tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

        Ok(Self {
            network_synchronizer,
            metrics_handle,
        })
    }

    pub async fn await_completion(
        self,
    ) -> (
        Result<(), tokio::task::JoinError>,
        Result<(), tokio::task::JoinError>,
    ) {
        tokio::join!(
            self.network_synchronizer.await_completion(),
            self.metrics_handle
        )
    }
}

#[cfg(test)]
mod smoke_tests {
    use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration};

    use tempdir::TempDir;
    use tokio::time;

    use super::Validator;
    use dag::{
        committee::Committee,
        config::{self, ClientParameters, NodePrivateConfig, NodePublicConfig},
        metrics,
        types::AuthorityIndex,
    };

    /// Check whether the validator specified by its metrics address has committed at least once.
    async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
        let route = metrics::server::METRICS_ROUTE;
        let res = reqwest::get(format! {"http://{address}{route}"}).await?;
        let string = res.text().await?;
        let commit = string.contains("committed_leaders_total");
        Ok(commit)
    }

    /// Await for all the validators specified by their metrics addresses to commit.
    async fn await_for_commits(addresses: Vec<SocketAddr>) {
        let mut queue = VecDeque::from(addresses);
        while let Some(address) = queue.pop_front() {
            time::sleep(Duration::from_millis(100)).await;
            match check_commit(&address).await {
                Ok(commits) if commits => (),
                _ => queue.push_back(address),
            }
        }
    }

    /// Ensure that a committee of honest validators commits.
    #[tokio::test]
    async fn validator_commit() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(0);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_commit").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        for (i, private_config) in private_configs.into_iter().enumerate() {
            let authority = i as AuthorityIndex;

            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 5;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    /// Ensure validators can sync missing blocks
    #[tokio::test]
    async fn validator_sync() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(100);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_sync").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        // Boot all validators but one.
        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }
            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        // Boot the last validator after they others commit.
        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }

        // Boot the last validator.
        let authority = 0;
        let private_config =
            NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size).remove(authority);
        let validator = Validator::start(
            authority as AuthorityIndex,
            committee.clone(),
            public_config.clone(),
            private_config,
            client_parameters,
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());

        // Ensure the last validator commits.
        let address = public_config
            .all_metric_addresses()
            .next()
            .map(|address| address.to_owned())
            .unwrap();
        let timeout = config::node_defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(vec![address]) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    // Ensure that honest validators commit despite the presence of a crash fault.
    #[tokio::test]
    async fn validator_crash_faults() {
        let committee_size = 4;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(200);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_crash_faults").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }

            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                authority,
                committee.clone(),
                public_config.clone(),
                private_config,
                client_parameters.clone(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::node_defaults::default_leader_timeout() * 15;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }
}

#[cfg(test)]
mod integration_tests {
    use std::time::Duration;

    use dag::{
        config::NodePublicConfig,
        context::TokioCtx,
        core::block_handler::CommitHandler,
        core::threshold_clock::threshold_clock_valid_non_genesis,
        data::Data,
        metrics::Metrics,
        sync::net_sync::NetworkSyncer,
        test_util::{check_commits, networks_and_addresses},
        types::{AuthorityIndex, StatementBlock},
    };
    use mysticeti_consensus::test_util::{committee_and_cores, committee_and_cores_persisted};
    use rand::{Rng, SeedableRng, prelude::StdRng};

    #[test]
    fn test_core_simple_exchange() {
        let (_committee, mut cores) = committee_and_cores::<TokioCtx>(4);

        let mut blocks = vec![];
        for core in &mut cores {
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            eprintln!("{}: {}", core.authority(), block);
            blocks.push(block.clone());
        }
        let more_blocks = blocks.split_off(1);

        eprintln!("===");

        let mut blocks_r2 = vec![];
        for core in &mut cores {
            core.add_blocks(blocks.clone());
            assert!(core.try_new_block().is_none());
            core.add_blocks(more_blocks.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority(), block);
            assert_eq!(block.reference().round, 2);
            blocks_r2.push(block.clone());
        }

        for core in &mut cores {
            core.add_blocks(blocks_r2.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority(), block);
            assert_eq!(block.reference().round, 3);
        }
    }

    #[test]
    fn test_randomized_simple_exchange() {
        for seed in 0..100u8 {
            let mut rng = StdRng::from_seed([seed; 32]);
            let (committee, mut cores) = committee_and_cores::<TokioCtx>(4);

            let mut pending: Vec<_> = committee.authorities().map(|_| vec![]).collect();
            for core in &mut cores {
                let block = core
                    .try_new_block()
                    .expect("Must be able to create block after genesis");
                assert_eq!(block.reference().round, 1);
                eprintln!("{}: {}", core.authority(), block);
                assert!(
                    threshold_clock_valid_non_genesis(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                push_all(&mut pending, core.authority(), &block);
            }
            let target_round = 10;
            for i in 0..1000 {
                let authority = committee.random_authority(&mut rng);
                let core = &mut cores[authority as usize];
                let this_pending = &mut pending[authority as usize];
                let c = rng.gen_range(1..4usize);
                let mut blocks = vec![];
                for _ in 0..c {
                    if this_pending.is_empty() {
                        break;
                    }
                    let block = this_pending.remove(rng.gen_range(0..this_pending.len()));
                    blocks.push(block);
                }
                if blocks.is_empty() {
                    continue;
                }
                core.add_blocks(blocks);
                let Some(block) = core.try_new_block() else {
                    continue;
                };
                assert!(
                    threshold_clock_valid_non_genesis(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                push_all(&mut pending, core.authority(), &block);
                if cores.iter().all(|c| c.last_proposed() >= target_round) {
                    println!(
                        "Seed {seed} succeed in {i} exchanges, \
                        all cores reached round {target_round}",
                    );
                    break;
                }
            }
            assert!(
                cores.iter().all(|c| c.last_proposed() >= target_round),
                "Seed {seed} failed - not all cores reached \
                round {target_round}",
            );
        }
    }

    #[test]
    fn test_core_recovery() {
        let tmp = tempdir::TempDir::new("test_core_recovery").unwrap();
        let (_committee, mut cores) =
            committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

        let mut blocks = vec![];
        for core in &mut cores {
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            eprintln!("{}: {}", core.authority(), block);
            blocks.push(block.clone());
        }
        drop(cores);

        let (_committee, mut cores) =
            committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

        let more_blocks = blocks.split_off(2);

        eprintln!("===");

        let mut blocks_r2 = vec![];
        for core in &mut cores {
            core.add_blocks(blocks.clone());
            assert!(core.try_new_block().is_none());
            core.add_blocks(more_blocks.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority(), block);
            assert_eq!(block.reference().round, 2);
            blocks_r2.push(block.clone());
        }

        drop(cores);

        eprintln!("===");

        let (_committee, mut cores) =
            committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

        for core in &mut cores {
            core.add_blocks(blocks_r2.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority(), block);
            assert_eq!(block.reference().round, 3);
        }
    }

    #[tokio::test]
    async fn test_network_sync() {
        let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
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
                &NodePublicConfig::new_for_tests(4),
            );
            network_syncers.push(network_syncer);
        }
        println!("Started");
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
    }

    fn push_all(
        p: &mut [Vec<Data<StatementBlock>>],
        except: AuthorityIndex,
        block: &Data<StatementBlock>,
    ) {
        for (i, q) in p.iter_mut().enumerate() {
            if i as AuthorityIndex != except {
                q.push(block.clone());
            }
        }
    }
}
