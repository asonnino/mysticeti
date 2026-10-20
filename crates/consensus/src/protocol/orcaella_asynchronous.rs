// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::{block::RoundNumber, committee::Stake};

use super::{LeaderElection, Protocol, ProtocolError};

impl Protocol {
    /// Orcaella (asynchronous variant): Orcaella's thresholds with one or two buffer
    /// rounds and no leader wait; derived in the unified-decider appendix of the lean-dag paper.
    pub fn orcaella_asynchronous(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Result<Self, ProtocolError> {
        ProtocolError::check_wave_length("Orcaella Async", wave_length, [3, 4])?;
        let orcaella =
            Self::orcaella(total_stake, f, c, leader_count).map_err(|error| match error {
                ProtocolError::FaultBoundViolated { n, min_n, .. } => {
                    ProtocolError::FaultBoundViolated {
                        protocol: "Orcaella Async",
                        n,
                        min_n,
                    }
                }
                error => error,
            })?;
        Ok(Self {
            wave_length,
            leader_wait: false,
            leader_election: LeaderElection::fake_coin(),
            ..orcaella
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::committee::Stake;

    use crate::{
        protocol::{ConsensusProtocol, Protocol, ProtocolError},
        test_scenarios::scenario_tests,
    };

    scenario_tests!(f0_c1_w3, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::OrcaellaAsynchronous { leader_count, f: 0, c: 1, wave_length: 3 }
    });
    // With f > 0, n = 4 is below the 5f + 3c + 1 bound.
    scenario_tests!(f1_c0_w3, sizes: [n20], |leader_count| {
        ConsensusProtocol::OrcaellaAsynchronous { leader_count, f: 1, c: 0, wave_length: 3 }
    });
    scenario_tests!(f1_c1_w3, sizes: [n20], |leader_count| {
        ConsensusProtocol::OrcaellaAsynchronous { leader_count, f: 1, c: 1, wave_length: 3 }
    });
    scenario_tests!(f1_c1_w4, sizes: [n20], |leader_count| {
        ConsensusProtocol::OrcaellaAsynchronous { leader_count, f: 1, c: 1, wave_length: 4 }
    });

    /// Async Orcaella keeps Orcaella's thresholds and only changes the wave length
    /// and the leader wait.
    #[test]
    fn orcaella_asynchronous_reduces_to_orcaella() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for (n, f, c) in [(3, 0, 1), (9, 1, 1), (20, 0, 1), (20, 3, 1)] {
            let orcaella = Protocol::orcaella(n, f, c, leader_count).unwrap();
            for wave_length in [3, 4] {
                let protocol =
                    Protocol::orcaella_asynchronous(n, f, c, leader_count, wave_length).unwrap();
                assert_eq!(protocol.direct_commit_quorum, orcaella.direct_commit_quorum);
                assert_eq!(protocol.direct_skip_quorum, orcaella.direct_skip_quorum);
                assert_eq!(protocol.certificate_quorum, orcaella.certificate_quorum);
                assert_eq!(protocol.quorum_threshold, orcaella.quorum_threshold);
                assert_eq!(protocol.anchor_link_size, orcaella.anchor_link_size);
                assert_eq!(protocol.require_crypto, orcaella.require_crypto);
                assert_eq!(protocol.leader_count, leader_count);
                assert!(protocol.fast_path.is_none());
                assert!(protocol.merged_certificates);
                assert!(protocol.pipeline);
                assert_eq!(protocol.wave_length, wave_length);
                assert!(!protocol.leader_wait);
            }
        }
    }

    /// Only one or two buffer rounds are accepted.
    #[test]
    fn orcaella_asynchronous_invalid_wave_length_rejected() {
        for wave_length in [0, 2, 5] {
            let result = Protocol::orcaella_asynchronous(
                9,
                1,
                1,
                NonZeroUsize::new(1).unwrap(),
                wave_length,
            );
            assert!(matches!(
                result,
                Err(ProtocolError::InvalidWaveLength {
                    protocol: "Orcaella Async",
                    wave_length: got,
                    allowed: [3, 4],
                }) if got == wave_length
            ));
        }
    }

    /// The Orcaella fault bound is enforced and reported under the async name.
    #[test]
    fn orcaella_asynchronous_fault_bound_rejected() {
        let leader_count = NonZeroUsize::new(1).unwrap();
        let result = Protocol::orcaella_asynchronous(5, 1, 0, leader_count, 3);
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "Orcaella Async",
                n: 5,
                min_n: 6,
            })
        ));
        let result = Protocol::orcaella_asynchronous(2, 0, 1, leader_count, 4);
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "Orcaella Async",
                n: 2,
                min_n: 3,
            })
        ));
    }

    /// Fault stakes whose bound overflows `Stake` are rejected, not wrapped.
    #[test]
    fn orcaella_asynchronous_bound_overflow_rejected() {
        let leader_count = NonZeroUsize::new(1).unwrap();
        for (f, c) in [(Stake::MAX / 5 + 1, 0), (0, Stake::MAX / 2 + 1)] {
            let result = Protocol::orcaella_asynchronous(100, f, c, leader_count, 3);
            assert!(matches!(
                result,
                Err(ProtocolError::FaultBoundViolated { .. })
            ));
        }
    }
}
