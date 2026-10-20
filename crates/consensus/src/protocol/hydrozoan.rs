// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{FastPath, LeaderElection, Protocol, ProtocolError};

impl Protocol {
    /// Hydrozoan
    ///
    /// "Hydrozoan: Latency-Adaptive DAG Consensus under Mixed Byzantine and Crash Faults"
    /// <https://sonnino.com/papers/hydrozoan.pdf>
    pub fn hydrozoan(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        k: Stake,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        // Widen to u128 so the bound and threshold arithmetic cannot overflow;
        // every resulting threshold is at most `total_stake`, so narrowing back
        // to `Stake` is safe.
        let n = total_stake as u128;
        let (f, c, k) = (f as u128, c as u128, k as u128);
        let min_n = 3 * f + 2 * c + k + 1;
        if min_n > n {
            return Err(ProtocolError::FaultBoundViolated {
                protocol: "Hydrozoan",
                n: total_stake,
                min_n: Stake::try_from(min_n).unwrap_or(Stake::MAX),
            });
        }

        let p = (c + k) / 2;
        // ceil((n + f + 1) / 2): large enough that two conflicting certificates
        // cannot both form.
        let certificate_quorum = (n + f + 2) / 2;
        let fast_path = FastPath {
            commit_quorum: (n - p) as Stake,
            weak_indirect_quorum: (f + p + 1) as Stake,
        };
        // Certificate uniqueness, and a fast commit starving every conflicting
        // leader block below the weak indirect quorum. Both follow from the fault
        // bound; assert on the stored thresholds to catch formula regressions.
        assert!(2 * certificate_quorum > n + f);
        let fast_commit_quorum = fast_path.commit_quorum as u128;
        let weak_indirect_quorum = fast_path.weak_indirect_quorum as u128;
        assert!(fast_commit_quorum + weak_indirect_quorum > n + f);

        Ok(Self {
            direct_commit_quorum: (2 * f + c + 1) as Stake,
            direct_skip_quorum: fast_path.commit_quorum,
            certificate_quorum: certificate_quorum as Stake,
            quorum_threshold: (n - f - c) as Stake,
            fast_path: Some(fast_path),
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
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

    use crate::{
        protocol::{ConsensusProtocol, Protocol, ProtocolError},
        test_scenarios::scenario_tests,
    };

    scenario_tests!(f1_c0_k0, sizes: [n4, n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 1, c: 0, k: 0 }
    });
    scenario_tests!(f0_c1_k1, sizes: [n4, n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 0, c: 1, k: 1 }
    });
    // Larger fault and slack configurations, above the 3f + 2c + k + 1 bound only at n = 20.
    scenario_tests!(f6_c0_k1, sizes: [n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 6, c: 0, k: 1 }
    });
    scenario_tests!(f0_c9_k1, sizes: [n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 0, c: 9, k: 1 }
    });
    scenario_tests!(f3_c4_k2, sizes: [n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 3, c: 4, k: 2 }
    });
    scenario_tests!(f4_c1_k5, sizes: [n20], fast_path, |leader_count| {
        ConsensusProtocol::Hydrozoan { leader_count, f: 4, c: 1, k: 5 }
    });

    /// Reference thresholds from the design note at the mixed configuration
    /// n = 20, f = 3, c = 4, k = 2 (p = 3), where the certificate, slow-commit,
    /// and pacemaker quorums are all distinct.
    #[test]
    fn hydrozoan_thresholds_mixed_configuration() {
        let protocol = Protocol::hydrozoan(20, 3, 4, 2, NonZeroUsize::new(1).unwrap()).unwrap();
        let fast_path = protocol.fast_path.expect("dual-path protocol");
        assert_eq!(fast_path.commit_quorum, 17); // n - p
        assert_eq!(fast_path.weak_indirect_quorum, 7); // f + p + 1
        assert_eq!(protocol.certificate_quorum, 12); // ceil((n + f + 1) / 2)
        assert_eq!(protocol.direct_commit_quorum, 11); // 2f + c + 1
        assert_eq!(protocol.direct_skip_quorum, 17); // n - p
        assert_eq!(protocol.quorum_threshold, 13); // n - f - c
    }

    /// At `k = 0` the certificate, slow-commit, and pacemaker quorums coincide
    /// (n = 99, f = 10, c = 34 from the design note).
    #[test]
    fn hydrozoan_quorums_coincide_at_zero_slack() {
        let protocol = Protocol::hydrozoan(99, 10, 34, 0, NonZeroUsize::new(1).unwrap()).unwrap();
        assert_eq!(protocol.certificate_quorum, 55);
        assert_eq!(protocol.direct_commit_quorum, 55);
        assert_eq!(protocol.quorum_threshold, 55);
    }

    /// The fault bound `n >= 3f + 2c + k + 1` is enforced.
    #[test]
    fn hydrozoan_fault_bound_rejected() {
        let result = Protocol::hydrozoan(4, 1, 0, 1, NonZeroUsize::new(1).unwrap());
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "Hydrozoan",
                n: 4,
                min_n: 5,
            })
        ));
    }
}
