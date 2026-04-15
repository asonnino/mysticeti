// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::types::Stake;

/// Protocol-specific quorum thresholds. Each consensus
/// protocol defines its own thresholds based on total stake
/// and its fault tolerance assumptions.
pub struct ProtocolThresholds {
    strong_quorum: Stake,
    weak_quorum: Stake,
}

impl ProtocolThresholds {
    /// Thresholds for Mysticeti (n = 3f + 1).
    /// Strong quorum: 2f + 1, weak quorum: f + 1.
    pub fn mysticeti(total_stake: Stake) -> Self {
        Self {
            strong_quorum: 2 * total_stake / 3 + 1,
            weak_quorum: total_stake / 3 + 1,
        }
    }

    pub fn strong_quorum(&self) -> Stake {
        self.strong_quorum
    }

    pub fn weak_quorum(&self) -> Stake {
        self.weak_quorum
    }
}
