// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orcaella integration tests (baseline `c = 0`, pure BFT configuration).

use std::num::NonZeroUsize;

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

/// Crash-fault stake. `c = 0` yields the pure BFT configuration:
/// `direct_commit_quorum = direct_skip_quorum = total_stake` (unanimous),
/// `anchor_link_size = 2`.
const CRASH_STAKE: u64 = 0;

struct OrcaellaFixture;
impl Fixture for OrcaellaFixture {
    const COMMITTEE_SIZE: usize = 4;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::orcaella(
            committee.total_stake(),
            CRASH_STAKE,
            NonZeroUsize::new(1).unwrap(),
        )
        .expect("valid crash-fault stake")
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<OrcaellaFixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<OrcaellaFixture>();
}
