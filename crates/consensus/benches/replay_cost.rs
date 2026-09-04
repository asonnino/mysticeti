// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cost of one adaptive-period update (paper claim:replay): collecting the
//! interval anchor's window and scoring every candidate period. Run with
//! `cargo bench -p consensus`.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use consensus::{
    protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair},
    replay::{ReplayParams, choose_period, collect_window},
};
use criterion::{Criterion, criterion_group, criterion_main};
use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    committee::Committee,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee, drop_leader},
};

fn replay_params(committee: &Arc<Committee>, interval: u64, max_period: u64) -> ReplayParams {
    let spec = ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: None,
        async_wave_length: 5,
        adaptive: Some(AdaptiveConfig {
            interval,
            max_period: NonZeroU64::new(max_period).unwrap(),
            epsilon_percent: 10,
        }),
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(2).unwrap(),
    };
    let protocol = spec.to_protocol(committee).expect("valid protocol");
    ReplayParams::from_protocol(&protocol, committee.clone()).expect("steelhead protocol")
}

/// The targeted-delay attack shape: every round's blocks exclude the previous
/// round's round-robin leader, so replayed sync slots exercise the expensive
/// blame/skip paths (the case the memoized support maps exist for).
fn build_starved_dag(
    committee: &Arc<Committee>,
    storage: &mut Storage,
    depth: RoundNumber,
) -> Vec<BlockReference> {
    let n = committee.len() as u64;
    let mut references = build_dag(committee, storage, None, 0);
    for round in 1..=depth {
        let parents = if round == 1 {
            references.clone()
        } else {
            drop_leader(&references, Authority::new((round - 1) % n))
        };
        references = build_dag_layer(
            committee
                .authorities()
                .map(|authority| (authority, parents.clone()))
                .collect(),
            storage,
        );
    }
    references
}

fn bench_update(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("replay_update");
    group.sample_size(10);
    for (n, interval, max_period) in [(10, 32, 8), (50, 32, 8), (100, 32, 8), (100, 100, 16)] {
        for starved in [false, true] {
            let members = committee(n);
            let mut storage = Storage::new_for_test(&members);
            let depth = interval + 1;
            let top = if starved {
                build_starved_dag(&members, &mut storage, depth)
            } else {
                build_dag(&members, &mut storage, None, depth)
            };
            let anchor = storage
                .block_reader()
                .get_block(top[0])
                .expect("anchor exists");
            let params = replay_params(&members, interval, max_period);
            let current = NonZeroU64::new(max_period).unwrap();
            let shape = if starved { "starved" } else { "healthy" };
            group.bench_function(format!("n{n}-i{interval}-{shape}"), |bencher| {
                bencher.iter(|| {
                    let window = collect_window(storage.block_reader(), &anchor, interval);
                    choose_period(&window, &params, current, current, 10)
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_update);
criterion_main!(benches);
