// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mysticeti integration tests (single-leader pipelined `wave_length = 3`).

use std::num::NonZeroUsize;

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

struct MysticetiFixture;
impl Fixture for MysticetiFixture {
    const COMMITTEE_SIZE: usize = 4;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::mysticeti(committee.total_stake(), NonZeroUsize::new(1).unwrap())
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<MysticetiFixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<MysticetiFixture>();
}
