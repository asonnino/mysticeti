// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{LeaderElection, Protocol, ProtocolError};

impl Protocol {
    /// Hybrid Mysticeti: Mysticeti's three-round wave under `f` Byzantine and `c` crash faults;
    /// derived in the unified-decider appendix of the lean-dag paper (the skip threshold is not
    /// covered by the Lean proofs).
    pub fn hybrid_mysticeti(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        // Widen to u128 so the bound and threshold arithmetic cannot overflow; every resulting
        // threshold is at most `total_stake`, so narrowing back to `Stake` is safe.
        let n = total_stake as u128;
        let (f, c) = (f as u128, c as u128);
        let min_n = 3 * f + 2 * c + 1;
        if min_n > n {
            return Err(ProtocolError::FaultBoundViolated {
                protocol: "Hybrid Mysticeti",
                n: total_stake,
                min_n: Stake::try_from(min_n).unwrap_or(Stake::MAX),
            });
        }

        // ceil((n + f + 1) / 2): two conflicting certificates cannot both form.
        let certificate_quorum = (n + f + 2) / 2;
        // floor((n + f + 1) / 2): a direct skip and a certificate cannot both form.
        let direct_skip_quorum = (n + f).div_ceil(2);
        Ok(Self {
            direct_commit_quorum: (2 * f + c + 1) as Stake,
            direct_skip_quorum: direct_skip_quorum as Stake,
            certificate_quorum: certificate_quorum as Stake,
            quorum_threshold: (n - f - c) as Stake,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
            union_certificates: false,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: f != 0,
            leader_election: LeaderElection::RoundRobin,
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

    scenario_tests!(f1_c0, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 1, c: 0 }
    });
    scenario_tests!(f0_c1, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 0, c: 1 }
    });
    // With f, c > 0, n = 4 is below the 3f + 2c + 1 bound.
    scenario_tests!(f1_c1, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 1, c: 1 }
    });
    scenario_tests!(f3_c4, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 3, c: 4 }
    });
    scenario_tests!(f6_c0, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 6, c: 0 }
    });
    scenario_tests!(f0_c9, sizes: [n20], |leader_count| {
        ConsensusProtocol::HybridMysticeti { leader_count, f: 0, c: 9 }
    });

    /// Without crash faults, at n = 3f + 1, every threshold equals Mysticeti's.
    #[test]
    fn hybrid_mysticeti_reduces_to_mysticeti() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for f in [1, 2, 5, 33] {
            let n = 3 * f + 1;
            let mysticeti = Protocol::mysticeti(n, leader_count);
            let protocol = Protocol::hybrid_mysticeti(n, f, 0, leader_count).unwrap();
            assert_eq!(
                protocol.direct_commit_quorum,
                mysticeti.direct_commit_quorum
            );
            assert_eq!(protocol.direct_skip_quorum, mysticeti.direct_skip_quorum);
            assert_eq!(protocol.certificate_quorum, mysticeti.certificate_quorum);
            assert_eq!(protocol.quorum_threshold, mysticeti.quorum_threshold);
            assert_eq!(protocol.anchor_link_size, mysticeti.anchor_link_size);
            assert_eq!(protocol.wave_length, mysticeti.wave_length);
            assert_eq!(protocol.merged_certificates, mysticeti.merged_certificates);
            assert_eq!(protocol.leader_wait, mysticeti.leader_wait);
        }
    }

    /// The commit, certificate and pacemaker quorums equal Hydrozoan's slow path at `k = 0`.
    #[test]
    fn hybrid_mysticeti_matches_hydrozoan_slow_path() {
        let leader_count = NonZeroUsize::new(1).unwrap();
        for (n, f, c) in [
            (4, 1, 0),
            (4, 0, 1),
            (20, 1, 1),
            (20, 3, 4),
            (20, 6, 0),
            (20, 0, 9),
        ] {
            let hydrozoan = Protocol::hydrozoan(n, f, c, 0, leader_count).unwrap();
            let protocol = Protocol::hybrid_mysticeti(n, f, c, leader_count).unwrap();
            assert_eq!(
                protocol.direct_commit_quorum,
                hydrozoan.direct_commit_quorum
            );
            assert_eq!(protocol.certificate_quorum, hydrozoan.certificate_quorum);
            assert_eq!(protocol.quorum_threshold, hydrozoan.quorum_threshold);
            assert!(protocol.fast_path.is_none());
        }
    }

    /// Reference thresholds at n = 20, f = 3, c = 4, where all four quorums are distinct.
    #[test]
    fn hybrid_mysticeti_thresholds_mixed_configuration() {
        let protocol = Protocol::hybrid_mysticeti(20, 3, 4, NonZeroUsize::new(1).unwrap()).unwrap();
        assert_eq!(protocol.direct_commit_quorum, 11); // 2f + c + 1
        assert_eq!(protocol.certificate_quorum, 12); // ceil((n + f + 1) / 2)
        assert_eq!(protocol.direct_skip_quorum, 12); // floor((n + f + 1) / 2)
        assert_eq!(protocol.quorum_threshold, 13); // n - f - c
    }

    /// The fault bound `n >= 3f + 2c + 1` is enforced.
    #[test]
    fn hybrid_mysticeti_fault_bound_rejected() {
        let result = Protocol::hybrid_mysticeti(5, 1, 1, NonZeroUsize::new(1).unwrap());
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "Hybrid Mysticeti",
                n: 5,
                min_n: 6,
            })
        ));
    }

    /// Fault stakes whose bound overflows `Stake` are rejected, not wrapped.
    #[test]
    fn hybrid_mysticeti_bound_overflow_rejected() {
        let leader_count = NonZeroUsize::new(1).unwrap();
        for (f, c) in [(Stake::MAX / 3 + 1, 0), (0, Stake::MAX / 2 + 1)] {
            let result = Protocol::hybrid_mysticeti(100, f, c, leader_count);
            assert!(matches!(
                result,
                Err(ProtocolError::FaultBoundViolated { .. })
            ));
        }
    }
}
