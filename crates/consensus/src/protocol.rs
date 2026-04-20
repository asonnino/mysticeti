// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, time::Duration};

use dag::{block::RoundNumber, committee::Stake};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// User-facing choice of consensus protocol variant.
///
/// Each variant encodes the parameters the user can tune; the ones
/// that are fixed by the protocol (wave length, pipelining, quorum
/// thresholds) are derived in `to_protocol`.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ConsensusProtocol {
    CordialMinersPartiallySynchronous,
    CordialMinersAsynchronous,
    Mysticeti {
        #[serde(default = "default_leader_count")]
        leader_count: NonZeroUsize,
    },
    Odontoceti {
        #[serde(default = "default_leader_count")]
        leader_count: NonZeroUsize,
    },
    MahiMahi {
        #[serde(default = "default_leader_count")]
        leader_count: NonZeroUsize,
        #[serde(deserialize_with = "deserialize_mahi_mahi_wave_length")]
        wave_length: RoundNumber,
    },
}

fn default_leader_count() -> NonZeroUsize {
    NonZeroUsize::new(2).unwrap()
}

fn deserialize_mahi_mahi_wave_length<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<RoundNumber, D::Error> {
    let value = RoundNumber::deserialize(deserializer)?;
    if value != 4 && value != 5 {
        return Err(D::Error::custom(format!(
            "MahiMahi wave_length must be 4 or 5, got {value}"
        )));
    }
    Ok(value)
}

impl Default for ConsensusProtocol {
    fn default() -> Self {
        Self::Mysticeti {
            leader_count: default_leader_count(),
        }
    }
}

impl ConsensusProtocol {
    /// Build the concrete `Protocol` used by the committer.
    pub fn to_protocol(&self, total_stake: Stake) -> Protocol {
        match *self {
            Self::CordialMinersPartiallySynchronous => {
                Protocol::cordial_miners_partially_synchronous(total_stake)
            }
            Self::CordialMinersAsynchronous => Protocol::cordial_miners_asynchronous(total_stake),
            Self::Mysticeti { leader_count } => Protocol::mysticeti(total_stake, leader_count),
            Self::Odontoceti { leader_count } => Protocol::odontoceti(total_stake, leader_count),
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => Protocol::mahi_mahi(total_stake, leader_count, wave_length),
        }
    }

    /// Wave length dictated by the chosen protocol.
    pub fn wave_length(&self) -> RoundNumber {
        match *self {
            Self::Mysticeti { .. } => 3,
            Self::Odontoceti { .. } => 2,
            Self::MahiMahi { wave_length, .. } => wave_length,
            Self::CordialMinersPartiallySynchronous => 3,
            Self::CordialMinersAsynchronous => 5,
        }
    }
}

/// Protocol-specific parameters for the consensus committer.
pub struct Protocol {
    /// The strong quorum threshold to directly commit a leader.
    pub strong_quorum: Stake,
    /// The weak quorum to indirectly commit a leader.
    pub weak_quorum: Stake,
    /// The number of rounds to commit a leader.
    pub wave_length: RoundNumber,
    /// The number of leaders per round.
    pub leader_count: NonZeroUsize,
    /// Whether the protocol commits one leader per round.
    pub pipeline: bool,
    /// Whether the protocol should wait for leader blocks before proposing.
    pub leader_wait: bool,
    /// Whether to perform real cryptographic operations.
    pub require_crypto: bool,
}

impl Protocol {
    /// Cordial Miners (partially synchronous version)
    ///
    /// "Cordial Miners: Fast and Efficient Consensus for Every Eventuality" DISC 2023
    /// <https://arxiv.org/abs/2205.09174>
    pub fn cordial_miners_partially_synchronous(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Cordial Miners (asynchronous version)
    ///
    /// "Cordial Miners: Fast and Efficient Consensus for Every Eventuality" DISC 2023
    /// <https://arxiv.org/abs/2205.09174>
    pub fn cordial_miners_asynchronous(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 5,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: true,
        }
    }

    /// Mysticeti
    ///
    /// "Mysticeti: Reaching the Latency Limits with Uncertified DAGs" NDSS 2025.
    /// <https://sonnino.com/papers/mysticeti.pdf>
    pub fn mysticeti(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Odontoceti
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn odontoceti(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        Self {
            strong_quorum: 4 * total_stake / 5 + 1,
            weak_quorum: 2 * total_stake / 5 + 1,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Mahi-Mahi
    ///
    /// "Mahi-Mahi: Low-Latency Asynchronous BFT DAG-Based Consensus" ICDCS 2025.
    /// <https://sonnino.com/papers/mahi-mahi.pdf>
    pub fn mahi_mahi(
        total_stake: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Self {
        assert!(
            wave_length == 4 || wave_length == 5,
            "MahiMahi requires wave_length of 4 or 5"
        );
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length,
            leader_count,
            pipeline: true,
            leader_wait: false,
            require_crypto: true,
        }
    }
}

impl Protocol {
    /// Sensible default round timeout based on whether the protocol
    /// waits for a specific leader (slower, partially synchronous) or
    /// for a quorum of any blocks (faster, asynchronous).
    pub fn default_round_timeout(&self) -> Duration {
        if self.leader_wait {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(75)
        }
    }
}
