// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::{committee_and_cores, committee_and_cores_persisted, open_core_with_sender};
use dag::{
    authority::Authority,
    block::{Block, transaction::Transaction},
    context::TokioCtx,
    core::threshold_clock::threshold_clock_valid_non_genesis,
    data::Data,
    test_util::committee,
};
use minibytes::Bytes;
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
        let quorum_threshold = 2 * committee.total_stake() / 3 + 1;

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
            let core = &mut cores[authority.index()];
            let this_pending = &mut pending[authority.index()];
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
    let tmp = tempfile::TempDir::new().unwrap();
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

/// A replica that restarts with queued-but-unproposed transactions in its WAL must
/// account for them: they are included in its next block, and it must keep accepting
/// new transactions afterwards.
#[test]
fn test_core_recovery_with_unproposed_payload() {
    let tmp = tempfile::TempDir::new().unwrap();
    let committee = committee(4);
    let authority = Authority::from(0usize);
    let tx = |data: &'static [u8]| Transaction::new(Bytes::from(data));

    let other_blocks: Vec<_> = (1..4usize)
        .map(|i| {
            let (mut core, _sender) =
                open_core_with_sender::<TokioCtx>(Authority::from(i), &committee, None);
            core.try_new_block()
                .expect("Must be able to create block after genesis")
        })
        .collect();

    // Propose round 1, then queue a payload in the WAL without proposing it.
    let (mut core, sender) =
        open_core_with_sender::<TokioCtx>(authority, &committee, Some(tmp.path()));
    core.try_new_block()
        .expect("Must be able to create block after genesis");
    sender.try_send(vec![tx(b"tx-0"), tx(b"tx-1")]).unwrap();
    core.add_blocks(vec![]);
    drop(core);

    // The recovered payload goes into the next proposal.
    let (mut core, sender) =
        open_core_with_sender::<TokioCtx>(authority, &committee, Some(tmp.path()));
    core.add_blocks(other_blocks);
    let block = core
        .try_new_block()
        .expect("Must be able to create block after full round");
    assert_eq!(block.reference().round, 2);
    assert_eq!(block.transactions().len(), 2);

    // New transactions are still accepted.
    sender.try_send(vec![tx(b"tx-2")]).unwrap();
    let accepted = core.block_handler_mut().handle_blocks(true);
    assert_eq!(accepted.len(), 1);
}

fn push_all(p: &mut [Vec<Data<Block>>], except: Authority, block: &Data<Block>) {
    for (i, q) in p.iter_mut().enumerate() {
        if Authority::from(i) != except {
            q.push(block.clone());
        }
    }
}
