// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::{block::RoundNumber, committee::Stake};

use super::{LeaderElection, Protocol, ProtocolError};

impl Protocol {
    /// Hybrid Mahi-Mahi: Hybrid Mysticeti's thresholds on Mahi-Mahi's wave (one or two buffer
    /// rounds, no leader wait); derived in the unified-decider appendix of the lean-dag paper.
    pub fn hybrid_mahi_mahi(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Result<Self, ProtocolError> {
        ProtocolError::check_wave_length("Hybrid Mahi-Mahi", wave_length, [4, 5])?;
        let hybrid_mysticeti = Self::hybrid_mysticeti(total_stake, f, c, leader_count).map_err(
            |error| match error {
                ProtocolError::FaultBoundViolated { n, min_n, .. } => {
                    ProtocolError::FaultBoundViolated {
                        protocol: "Hybrid Mahi-Mahi",
                        n,
                        min_n,
                    }
                }
                error => error,
            },
        )?;
        Ok(Self {
            wave_length,
            leader_wait: false,
            leader_election: LeaderElection::fake_coin(),
            ..hybrid_mysticeti
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

    scenario_tests!(f1_c0_w4, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 1, c: 0, wave_length: 4 }
    });
    scenario_tests!(f0_c1_w4, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 0, c: 1, wave_length: 4 }
    });
    // With f, c > 0, n = 4 is below the 3f + 2c + 1 bound.
    scenario_tests!(f1_c1_w4, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 1, c: 1, wave_length: 4 }
    });
    scenario_tests!(f3_c4_w4, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 3, c: 4, wave_length: 4 }
    });
    scenario_tests!(f6_c0_w4, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 6, c: 0, wave_length: 4 }
    });
    scenario_tests!(f0_c9_w4, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 0, c: 9, wave_length: 4 }
    });
    // Wave 5 is the slowest geometry to test, so it covers the Mahi-Mahi endpoint, the tight
    // f = 0 case and one mixed configuration.
    scenario_tests!(f1_c0_w5, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 1, c: 0, wave_length: 5 }
    });
    scenario_tests!(f0_c1_w5, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 0, c: 1, wave_length: 5 }
    });
    scenario_tests!(f3_c4_w5, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMahiMahi { leader_count, f: 3, c: 4, wave_length: 5 }
    });

    /// Hybrid Mahi-Mahi keeps Hybrid Mysticeti's thresholds and only changes the wave length and
    /// the leader wait.
    #[test]
    fn hybrid_mahi_mahi_reduces_to_hybrid_mysticeti() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for (n, f, c) in [(4, 1, 0), (4, 0, 1), (20, 1, 1), (20, 3, 4), (20, 0, 9)] {
            let hybrid_mysticeti = Protocol::hybrid_mysticeti(n, f, c, leader_count).unwrap();
            for wave_length in [4, 5] {
                let protocol =
                    Protocol::hybrid_mahi_mahi(n, f, c, leader_count, wave_length).unwrap();
                assert_eq!(
                    protocol.direct_commit_quorum,
                    hybrid_mysticeti.direct_commit_quorum
                );
                assert_eq!(
                    protocol.direct_skip_quorum,
                    hybrid_mysticeti.direct_skip_quorum
                );
                assert_eq!(
                    protocol.certificate_quorum,
                    hybrid_mysticeti.certificate_quorum
                );
                assert_eq!(protocol.quorum_threshold, hybrid_mysticeti.quorum_threshold);
                assert_eq!(protocol.anchor_link_size, hybrid_mysticeti.anchor_link_size);
                assert_eq!(protocol.require_crypto, hybrid_mysticeti.require_crypto);
                assert!(protocol.fast_path.is_none());
                assert!(!protocol.merged_certificates);
                assert!(protocol.pipeline);
                assert_eq!(protocol.wave_length, wave_length);
                assert!(!protocol.leader_wait);
            }
        }
    }

    /// Without crash faults, at n = 3f + 1, every threshold equals Mahi-Mahi's.
    #[test]
    fn hybrid_mahi_mahi_reduces_to_mahi_mahi() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for f in [1, 2, 5, 33] {
            let n = 3 * f + 1;
            for wave_length in [4, 5] {
                let mahi_mahi = Protocol::mahi_mahi(n, leader_count, wave_length).unwrap();
                let protocol =
                    Protocol::hybrid_mahi_mahi(n, f, 0, leader_count, wave_length).unwrap();
                assert_eq!(
                    protocol.direct_commit_quorum,
                    mahi_mahi.direct_commit_quorum
                );
                assert_eq!(protocol.direct_skip_quorum, mahi_mahi.direct_skip_quorum);
                assert_eq!(protocol.certificate_quorum, mahi_mahi.certificate_quorum);
                assert_eq!(protocol.quorum_threshold, mahi_mahi.quorum_threshold);
                assert_eq!(protocol.wave_length, mahi_mahi.wave_length);
                assert_eq!(protocol.leader_wait, mahi_mahi.leader_wait);
            }
        }
    }

    /// Only one or two buffer rounds are accepted.
    #[test]
    fn hybrid_mahi_mahi_invalid_wave_length_rejected() {
        for wave_length in [0, 3, 6] {
            let result =
                Protocol::hybrid_mahi_mahi(20, 3, 4, NonZeroUsize::new(1).unwrap(), wave_length);
            assert!(matches!(
                result,
                Err(ProtocolError::InvalidWaveLength {
                    protocol: "Hybrid Mahi-Mahi",
                    wave_length: got,
                    allowed: [4, 5],
                }) if got == wave_length
            ));
        }
    }

    /// The Hybrid Mysticeti fault bound is enforced and reported under this protocol's name.
    #[test]
    fn hybrid_mahi_mahi_fault_bound_rejected() {
        let result = Protocol::hybrid_mahi_mahi(5, 1, 1, NonZeroUsize::new(1).unwrap(), 4);
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "Hybrid Mahi-Mahi",
                n: 5,
                min_n: 6,
            })
        ));
    }

    /// Fault stakes whose bound overflows `Stake` are rejected, not wrapped.
    #[test]
    fn hybrid_mahi_mahi_bound_overflow_rejected() {
        let leader_count = NonZeroUsize::new(1).unwrap();
        for (f, c) in [(Stake::MAX / 3 + 1, 0), (0, Stake::MAX / 2 + 1)] {
            let result = Protocol::hybrid_mahi_mahi(100, f, c, leader_count, 4);
            assert!(matches!(
                result,
                Err(ProtocolError::FaultBoundViolated { .. })
            ));
        }
    }
}
