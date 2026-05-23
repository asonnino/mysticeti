// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Nemo-Nemo integration tests.

use std::num::NonZeroUsize;

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

struct NemoNemoFixture;
impl Fixture for NemoNemoFixture {
    const COMMITTEE_SIZE: usize = 4;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::nemo_nemo(committee.total_stake(), NonZeroUsize::new(1).unwrap())
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<NemoNemoFixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<NemoNemoFixture>();
}
