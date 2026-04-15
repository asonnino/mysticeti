// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use consensus::test_util::{committee_and_cores, committee_and_cores_persisted};
use consensus::thresholds::ProtocolThresholds;
use dag::{
    context::TokioCtx,
    core::threshold_clock::threshold_clock_valid_non_genesis,
    data::Data,
    types::{AuthorityIndex, StatementBlock},
};
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
        let quorum_threshold =
            ProtocolThresholds::mysticeti(committee.total_stake()).strong_quorum();

        let mut pending: Vec<_> = committee.authorities().map(|_| vec![]).collect();
        for core in &mut cores {
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            eprintln!("{}: {}", core.authority(), block);
            assert!(
                threshold_clock_valid_non_genesis(&block, &committee, quorum_threshold),
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
                threshold_clock_valid_non_genesis(&block, &committee, quorum_threshold),
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
    let (_committee, mut cores) = committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

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

    let (_committee, mut cores) = committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

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

    let (_committee, mut cores) = committee_and_cores_persisted::<TokioCtx>(4, Some(tmp.path()));

    for core in &mut cores {
        core.add_blocks(blocks_r2.clone());
        let block = core
            .try_new_block()
            .expect("Must be able to create block after full round");
        eprintln!("{}: {}", core.authority(), block);
        assert_eq!(block.reference().round, 3);
    }
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
