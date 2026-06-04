// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, num::NonZeroUsize, time::Duration};

use dag::{
    block::RoundNumber,
    committee::{Committee, Stake},
};
use serde::{Deserialize, Serialize};

/// User-facing choice of consensus protocol variant.
///
/// Each variant encodes the parameters the user can tune; the ones
/// that are fixed by the protocol (wave length, pipelining, quorum
/// thresholds) are derived in `to_protocol`.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "protocol", rename_all = "kebab-case")]
pub enum ConsensusProtocol {
    CordialMinersPartiallySynchronous,
    CordialMinersAsynchronous,
    Mysticeti {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
    BlueBottle {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
    Orcaella {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        /// Byzantine-fault stake to tolerate.
        f: Stake,
        /// Crash-only fault stake to tolerate (on top of `f`); requires
        /// `5f + 3c + 1 <= total_stake`.
        c: Stake,
    },
    MahiMahi {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        #[serde(deserialize_with = "defaults::deserialize_mahi_mahi_wave_length")]
        wave_length: RoundNumber,
    },
    NemoNemo {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
}

mod defaults {
    use std::num::NonZeroUsize;

    use dag::block::RoundNumber;
    use serde::{Deserialize, Deserializer, de::Error as _};

    pub fn default_leader_count() -> NonZeroUsize {
        NonZeroUsize::new(2).unwrap()
    }

    pub fn deserialize_mahi_mahi_wave_length<'de, D: Deserializer<'de>>(
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
}

impl Default for ConsensusProtocol {
    fn default() -> Self {
        Self::Mysticeti {
            leader_count: defaults::default_leader_count(),
        }
    }
}

impl fmt::Display for ConsensusProtocol {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CordialMinersPartiallySynchronous => {
                write!(fmt, "Cordial Miners (PS)")
            }
            Self::CordialMinersAsynchronous => write!(fmt, "Cordial Miners (Async)"),
            Self::Mysticeti { leader_count } => {
                write!(fmt, "Mysticeti ({} leaders/round)", leader_count)
            }
            Self::BlueBottle { leader_count } => {
                write!(fmt, "Blue Bottle ({} leaders/round)", leader_count)
            }
            Self::Orcaella { leader_count, f, c } => {
                write!(
                    fmt,
                    "Orcaella ({} leaders/round, f={f}, c={c})",
                    leader_count
                )
            }
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => write!(
                fmt,
                "Mahi-Mahi ({} leaders/round, wave length {})",
                leader_count, wave_length
            ),
            Self::NemoNemo { leader_count } => {
                write!(fmt, "Nemo-Nemo ({} leaders/round)", leader_count)
            }
        }
    }
}

impl ConsensusProtocol {
    /// Build the concrete `Protocol` used by the committer.
    pub fn to_protocol(&self, committee: &Committee) -> Result<Protocol, ProtocolError> {
        let total_stake = committee.total_stake();
        let committee_size = committee.len();
        let user_leader_count = match self {
            Self::CordialMinersPartiallySynchronous | Self::CordialMinersAsynchronous => None,
            Self::Mysticeti { leader_count }
            | Self::BlueBottle { leader_count }
            | Self::Orcaella { leader_count, .. }
            | Self::MahiMahi { leader_count, .. }
            | Self::NemoNemo { leader_count } => Some(*leader_count),
        };
        if let Some(leader_count) = user_leader_count
            && leader_count.get() > committee_size
        {
            return Err(ProtocolError::LeaderCountExceedsCommittee {
                leader_count,
                committee_size,
            });
        }

        Ok(match *self {
            Self::CordialMinersPartiallySynchronous => {
                Protocol::cordial_miners_partially_synchronous(total_stake)
            }
            Self::CordialMinersAsynchronous => Protocol::cordial_miners_asynchronous(total_stake),
            Self::Mysticeti { leader_count } => Protocol::mysticeti(total_stake, leader_count),
            Self::BlueBottle { leader_count } => Protocol::blue_bottle(total_stake, leader_count),
            Self::Orcaella { leader_count, f, c } => {
                Protocol::orcaella(total_stake, f, c, leader_count)?
            }
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => Protocol::mahi_mahi(total_stake, leader_count, wave_length)?,
            Self::NemoNemo { leader_count } => Protocol::nemo_nemo(total_stake, leader_count),
        })
    }
}

/// Test-only constructors.
#[cfg(any(test, feature = "test-utils"))]
impl ConsensusProtocol {
    /// All protocols at the baseline matrix configuration used by the
    /// per-scenario integration tests, swept over `leader_counts`.
    pub fn all_for_test(n: usize, leader_counts: &[usize]) -> Vec<Self> {
        let mut variants = Vec::new();
        for &l in leader_counts {
            let leader_count = NonZeroUsize::new(l).expect("leader_count must be non-zero");
            variants.push(Self::Mysticeti { leader_count });
            variants.push(Self::BlueBottle { leader_count });
            variants.push(Self::NemoNemo { leader_count });
            variants.push(Self::Orcaella {
                leader_count,
                f: 0,
                c: 1,
            });
            if n >= 6 {
                variants.push(Self::Orcaella {
                    leader_count,
                    f: 1,
                    c: 0,
                });
            }
            if n >= 9 {
                variants.push(Self::Orcaella {
                    leader_count,
                    f: 1,
                    c: 1,
                });
            }
            variants.push(Self::MahiMahi {
                leader_count,
                wave_length: 4,
            });
            variants.push(Self::MahiMahi {
                leader_count,
                wave_length: 5,
            });
        }
        variants.push(Self::CordialMinersPartiallySynchronous);
        variants.push(Self::CordialMinersAsynchronous);
        variants
    }
}

/// Errors that can arise when building a [`Protocol`] from a [`ConsensusProtocol`].
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("Orcaella requires 5f + 3c + 1 <= n (n={n}, f={f}, c={c})")]
    OrcaellaFaultBoundViolated { n: Stake, f: Stake, c: Stake },
    #[error("Mahi-Mahi requires wave_length in {{4, 5}}, got {wave_length}")]
    MahiMahiInvalidWaveLength { wave_length: RoundNumber },
    #[error("leader_count ({leader_count}) exceeds committee size ({committee_size})")]
    LeaderCountExceedsCommittee {
        leader_count: NonZeroUsize,
        committee_size: usize,
    },
}

/// Protocol-specific parameters for the consensus committer.
pub struct Protocol {
    /// The quorum threshold to directly commit a leader.
    pub direct_commit_quorum: Stake,
    /// The quorum threshold to directly skip a leader.
    pub direct_skip_quorum: Stake,
    /// The stake of anchor-linked support required to indirectly decide a leader.
    pub anchor_link_size: Stake,
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
            direct_commit_quorum: quorum,
            direct_skip_quorum: total_stake,
            anchor_link_size: 1,
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
            direct_commit_quorum: quorum,
            direct_skip_quorum: total_stake,
            anchor_link_size: 1,
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
            direct_commit_quorum: quorum,
            direct_skip_quorum: quorum,
            anchor_link_size: 1,
            wave_length: 3,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Blue Bottle
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn blue_bottle(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let strong_quorum = 4 * total_stake / 5 + 1;
        let weak_quorum = 2 * total_stake / 5 + 1;
        Self {
            direct_commit_quorum: strong_quorum,
            direct_skip_quorum: strong_quorum,
            anchor_link_size: weak_quorum,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Orcaella
    ///
    /// "Orcaella: Hybrid Fault Tolerance with Client-Selectable Finality Latency"
    /// <TBD>
    pub fn orcaella(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        if 5 * f + 3 * c + 1 > total_stake {
            return Err(ProtocolError::OrcaellaFaultBoundViolated {
                n: total_stake,
                f,
                c,
            });
        }

        Ok(Self {
            direct_commit_quorum: total_stake - f - c,
            direct_skip_quorum: 4 * f + 2 * c + 1,
            anchor_link_size: total_stake - 3 * f - 2 * c,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        })
    }

    /// Mahi-Mahi
    ///
    /// "Mahi-Mahi: Low-Latency Asynchronous BFT DAG-Based Consensus" ICDCS 2025.
    /// <https://sonnino.com/papers/mahi-mahi.pdf>
    pub fn mahi_mahi(
        total_stake: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Result<Self, ProtocolError> {
        if wave_length != 4 && wave_length != 5 {
            return Err(ProtocolError::MahiMahiInvalidWaveLength { wave_length });
        }

        let quorum = 2 * total_stake / 3 + 1;
        Ok(Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: quorum,
            anchor_link_size: 1,
            wave_length,
            leader_count,
            pipeline: true,
            leader_wait: false,
            require_crypto: true,
        })
    }

    /// Nemo-Nemo
    ///
    /// "Finding Nemo-Nemo: CFT DAG-based Consensus in the WAN"
    /// <https://sonnino.com/papers/nemo-nemo.pdf>
    pub fn nemo_nemo(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let quorum = total_stake / 2 + 1;
        Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: total_stake,
            anchor_link_size: 1,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: false,
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
