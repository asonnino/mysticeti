// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! BlueBottle integration tests.
//!
//! BlueBottle (see [`Protocol::blue_bottle`]) is pipelined with `wave_length = 2`,
//! `strong_quorum = 4n/5 + 1`, and `anchor_link_size = 2n/5 + 1`. Tests use `n = 10`
//! so the integer quorums come out to clean values: `strong = 9`, `anchor_link = 5`.

use std::num::NonZeroUsize;

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

struct BlueBottleFixture;
impl Fixture for BlueBottleFixture {
    const COMMITTEE_SIZE: usize = 10;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::blue_bottle(committee.total_stake(), NonZeroUsize::new(1).unwrap())
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<BlueBottleFixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<BlueBottleFixture>();
}
