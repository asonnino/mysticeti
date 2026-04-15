// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod base_committer;
pub mod leader_election;
pub mod thresholds;
pub mod universal_committer;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_util;

#[cfg(test)]
mod tests;

use dag::types::RoundNumber;

/// Default wave length for all committers. A longer wave_length increases the
/// chance of committing the leader under asynchrony at the cost of latency in
/// the common case.
pub const DEFAULT_WAVE_LENGTH: RoundNumber = MINIMUM_WAVE_LENGTH;

/// We need at least one leader round, one voting round, and one decision round.
pub const MINIMUM_WAVE_LENGTH: RoundNumber = 3;
