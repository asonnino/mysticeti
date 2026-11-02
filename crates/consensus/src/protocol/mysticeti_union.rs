// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::Protocol;

impl Protocol {
    /// Mysticeti with union certificates: the thresholds and the three-round wave of Mysticeti,
    /// but a leader is committed from the union of the votes held by the decision-round blocks
    /// instead of from a single block that references a quorum of votes. The anchor of an
    /// indirect decision counts the votes in its causal history in the same way.
    ///
    /// A validator that holds decision-round blocks of stake `M` shares at least `M - 2f` stake of
    /// them with any later anchor, so it commits once every such set of blocks references a quorum
    /// of votes. Under minimum-quorum references (a leader timeout shorter than the link delay)
    /// no single block references a quorum of votes, and Mysticeti commits only sporadically.
    /// See `union_certificate` for the check.
    pub fn mysticeti_union(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        Self {
            union_certificates: true,
            ..Self::mysticeti(total_stake, leader_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::committee::Committee;

    use crate::{
        protocol::{ConsensusProtocol, Protocol, ProtocolError},
        test_scenarios::scenario_tests,
        union_certificate::MAX_AUTHORITIES,
    };

    scenario_tests!(baseline, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::MysticetiUnion { leader_count }
    });

    /// Only the certificate rule differs from Mysticeti.
    #[test]
    fn mysticeti_union_differs_from_mysticeti_only_in_certificates() {
        let leader_count = NonZeroUsize::new(2).unwrap();
        for n in [4, 20, 100] {
            let mysticeti = Protocol::mysticeti(n, leader_count);
            let protocol = Protocol::mysticeti_union(n, leader_count);
            assert!(protocol.union_certificates);
            assert!(!mysticeti.union_certificates);
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
            assert_eq!(protocol.pipeline, mysticeti.pipeline);
        }
    }

    /// The votes are indexed in `u64` masks, so a larger committee is refused up front.
    #[test]
    fn mysticeti_union_rejects_a_committee_wider_than_the_masks() {
        let spec = ConsensusProtocol::MysticetiUnion {
            leader_count: NonZeroUsize::new(1).unwrap(),
        };
        assert!(
            spec.to_protocol(&Committee::new_test(vec![1; MAX_AUTHORITIES]))
                .is_ok()
        );
        let error = spec
            .to_protocol(&Committee::new_test(vec![1; MAX_AUTHORITIES + 1]))
            .err();
        assert!(matches!(
            error,
            Some(ProtocolError::CommitteeTooLarge {
                max: MAX_AUTHORITIES,
                ..
            })
        ));
    }
}
