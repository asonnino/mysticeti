// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cordial Miners (Asynchronous) integration tests.

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

struct CordialMinersAsyncFixture;
impl Fixture for CordialMinersAsyncFixture {
    const COMMITTEE_SIZE: usize = 4;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::cordial_miners_asynchronous(committee.total_stake())
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<CordialMinersAsyncFixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<CordialMinersAsyncFixture>();
}
