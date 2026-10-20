// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{Protocol, ProtocolError};

impl Protocol {
    /// Orcaella
    ///
    /// "Orcaella: Hybrid Fault Tolerance with Client-Selectable Finality Latency"
    /// <https://sonnino.com/papers/orcaella.pdf>
    pub fn orcaella(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        let min_n = if f == 0 {
            2 * c as u128 + 1
        } else {
            5 * f as u128 + 3 * c as u128 + 1
        };
        if min_n > total_stake as u128 {
            return Err(ProtocolError::FaultBoundViolated {
                protocol: "Orcaella",
                n: total_stake,
                min_n: Stake::try_from(min_n).unwrap_or(Stake::MAX),
            });
        }

        Ok(Self {
            direct_commit_quorum: total_stake - f - c,
            direct_skip_quorum: 4 * f + 2 * c + 1,
            certificate_quorum: total_stake - f - c,
            quorum_threshold: total_stake - f - c,
            fast_path: None,
            anchor_link_size: total_stake - 3 * f - 2 * c,
            wave_length: 2,
            merged_certificates: true,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: f != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::committee::Stake;

    use crate::protocol::{Protocol, ProtocolError};

    /// A Byzantine stake whose bound `5f + 3c + 1` overflows `Stake` is rejected, not wrapped.
    #[test]
    fn orcaella_byzantine_bound_overflow_rejected() {
        let f = Stake::MAX / 5 + 1;
        let result = Protocol::orcaella(100, f, 0, NonZeroUsize::new(1).unwrap());
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated { .. })
        ));
    }

    /// A crash stake whose bound `2c + 1` overflows `Stake` is rejected, not wrapped.
    #[test]
    fn orcaella_crash_bound_overflow_rejected() {
        let c = Stake::MAX / 2 + 1;
        let result = Protocol::orcaella(100, 0, c, NonZeroUsize::new(1).unwrap());
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated { .. })
        ));
    }
}
