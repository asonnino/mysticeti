// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::{block::RoundNumber, committee::Stake};

use super::{LeaderElection, Protocol, ProtocolError};

impl Protocol {
    /// Nemo-Nemo (asynchronous variant): Nemo-Nemo's thresholds with no leader wait, the fake
    /// coin, and zero or one buffer round. With crash faults only, the coin keeps it live without
    /// buffer rounds; the buffer round only raises the worst-case commit probability.
    pub fn nemo_nemo_asynchronous(
        total_stake: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Result<Self, ProtocolError> {
        ProtocolError::check_wave_length("Nemo-Nemo Async", wave_length, [2, 3])?;
        Ok(Self {
            wave_length,
            leader_wait: false,
            leader_election: LeaderElection::fake_coin(),
            ..Self::nemo_nemo(total_stake, leader_count)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{
        leader::LeaderElection,
        protocol::{ConsensusProtocol, Protocol, ProtocolError},
        test_scenarios::scenario_tests,
    };

    scenario_tests!(w3, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::NemoNemoAsynchronous { leader_count, wave_length: 3 }
    });
    scenario_tests!(w2, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::NemoNemoAsynchronous { leader_count, wave_length: 2 }
    });

    /// Async Nemo-Nemo keeps Nemo-Nemo's thresholds and only changes the wave length, the leader
    /// wait and the leader election.
    #[test]
    fn nemo_nemo_asynchronous_reduces_to_nemo_nemo() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for n in [3, 4, 5, 20, 101] {
            let nemo_nemo = Protocol::nemo_nemo(n, leader_count);
            for wave_length in [2, 3] {
                let protocol =
                    Protocol::nemo_nemo_asynchronous(n, leader_count, wave_length).unwrap();
                assert_eq!(
                    protocol.direct_commit_quorum,
                    nemo_nemo.direct_commit_quorum
                );
                assert_eq!(protocol.direct_skip_quorum, nemo_nemo.direct_skip_quorum);
                assert_eq!(protocol.certificate_quorum, nemo_nemo.certificate_quorum);
                assert_eq!(protocol.quorum_threshold, nemo_nemo.quorum_threshold);
                assert_eq!(protocol.anchor_link_size, nemo_nemo.anchor_link_size);
                assert_eq!(protocol.require_crypto, nemo_nemo.require_crypto);
                assert_eq!(protocol.leader_count, leader_count);
                assert!(protocol.fast_path.is_none());
                assert!(protocol.merged_certificates);
                assert!(protocol.pipeline);
                assert_eq!(protocol.wave_length, wave_length);
                assert!(!protocol.leader_wait);
                assert_eq!(protocol.leader_election, LeaderElection::fake_coin());
            }
        }
    }

    /// Only zero or one buffer round is accepted.
    #[test]
    fn nemo_nemo_asynchronous_invalid_wave_length_rejected() {
        for wave_length in [0, 1, 4] {
            let result =
                Protocol::nemo_nemo_asynchronous(4, NonZeroUsize::new(1).unwrap(), wave_length);
            assert!(matches!(
                result,
                Err(ProtocolError::InvalidWaveLength {
                    protocol: "Nemo-Nemo Async",
                    wave_length: got,
                    allowed: [2, 3],
                }) if got == wave_length
            ));
        }
    }
}
