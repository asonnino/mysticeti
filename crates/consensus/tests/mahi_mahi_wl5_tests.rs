// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mahi-Mahi integration tests (`wave_length = 5`).

use std::num::NonZeroUsize;

use consensus::protocol::Protocol;
use dag::committee::Committee;

mod common;
use common::Fixture;

const WAVE_LENGTH: u64 = 5;

struct MahiMahiWl5Fixture;
impl Fixture for MahiMahiWl5Fixture {
    const COMMITTEE_SIZE: usize = 4;
    fn build_protocol(committee: &Committee) -> Protocol {
        Protocol::mahi_mahi(
            committee.total_stake(),
            NonZeroUsize::new(1).unwrap(),
            WAVE_LENGTH,
        )
        .expect("valid wave_length")
    }
}

#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    common::run_direct_commit::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn idempotence() {
    common::run_idempotence::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    common::run_multiple_direct_commit::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    common::run_direct_commit_late_call::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    common::run_no_genesis_commit::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn no_leader() {
    common::run_no_leader::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    common::run_direct_skip::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    common::run_indirect_commit::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    common::run_indirect_skip::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn undecided() {
    common::run_undecided::<MahiMahiWl5Fixture>();
}

#[test]
#[tracing_test::traced_test]
fn trailing_skip_not_re_yielded() {
    common::run_trailing_skip_not_re_yielded::<MahiMahiWl5Fixture>();
}
