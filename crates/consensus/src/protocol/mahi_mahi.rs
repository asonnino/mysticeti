// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::{block::RoundNumber, committee::Stake};

use super::{Protocol, ProtocolError};

impl Protocol {
    /// Mahi-Mahi
    ///
    /// "Mahi-Mahi: Low-Latency Asynchronous BFT DAG-Based Consensus" ICDCS 2025.
    /// <https://sonnino.com/papers/mahi-mahi.pdf>
    pub fn mahi_mahi(
        total_stake: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Result<Self, ProtocolError> {
        ProtocolError::check_wave_length("Mahi-Mahi", wave_length, [4, 5])?;

        let quorum = 2 * total_stake / 3 + 1;
        Ok(Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: quorum,
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length,
            merged_certificates: false,
            leader_count,
            pipeline: true,
            leader_wait: false,
            require_crypto: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{
        protocol::{ConsensusProtocol, Protocol},
        test_scenarios::scenario_tests,
    };

    scenario_tests!(w4, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::MahiMahi { leader_count, wave_length: 4 }
    });
    scenario_tests!(w5, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::MahiMahi { leader_count, wave_length: 5 }
    });

    /// Mahi-Mahi keeps its wave-length error message.
    #[test]
    fn mahi_mahi_invalid_wave_length_rejected() {
        let error = Protocol::mahi_mahi(4, NonZeroUsize::new(1).unwrap(), 3)
            .err()
            .expect("wave length 3 is invalid");
        assert_eq!(
            error.to_string(),
            "Mahi-Mahi requires wave_length in {4, 5}, got 3"
        );
    }
}
