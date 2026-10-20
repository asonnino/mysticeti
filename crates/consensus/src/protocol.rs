// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, num::NonZeroUsize, time::Duration};

use dag::{
    block::RoundNumber,
    committee::{Committee, Stake},
};
use serde::{Deserialize, Serialize};

mod blue_bottle;
mod blue_bottle_asynchronous;
mod cordial_miners;
mod cordial_miners_asynchronous;
mod hydrozoan;
mod mahi_mahi;
mod mysticeti;
mod nemo_nemo;
mod orcaella;
mod orcaella_asynchronous;

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
    BlueBottlePartiallySynchronous {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
    BlueBottleAsynchronous {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
    Orcaella {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        /// Byzantine-fault stake to tolerate.
        f: Stake,
        /// Crash-only fault stake to tolerate (on top of `f`); requires
        /// `n >= 5f+3c+1` for `f > 0`, or `n >= 2c+1` for `f = 0`.
        c: Stake,
    },
    OrcaellaAsynchronous {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        /// Byzantine-fault stake to tolerate.
        f: Stake,
        /// Crash-only fault stake to tolerate (on top of `f`); same bound as Orcaella.
        c: Stake,
        #[serde(deserialize_with = "defaults::deserialize_orcaella_asynchronous_wave_length")]
        wave_length: RoundNumber,
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
    Hydrozoan {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        /// Byzantine-fault stake to tolerate.
        f: Stake,
        /// Crash-only fault stake to tolerate (on top of `f`).
        c: Stake,
        /// Tunable slack widening the fast path; requires `n >= 3f + 2c + k + 1`.
        k: Stake,
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

    pub fn deserialize_orcaella_asynchronous_wave_length<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<RoundNumber, D::Error> {
        let value = RoundNumber::deserialize(deserializer)?;
        if value != 3 && value != 4 {
            return Err(D::Error::custom(format!(
                "OrcaellaAsynchronous wave_length must be 3 or 4, got {value}"
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
            Self::BlueBottlePartiallySynchronous { leader_count } => {
                write!(fmt, "Blue Bottle PS ({} leaders/round)", leader_count)
            }
            Self::BlueBottleAsynchronous { leader_count } => {
                write!(fmt, "Blue Bottle Async ({} leaders/round)", leader_count)
            }
            Self::Orcaella { leader_count, f, c } => {
                write!(
                    fmt,
                    "Orcaella ({} leaders/round, f={f}, c={c})",
                    leader_count
                )
            }
            Self::OrcaellaAsynchronous {
                leader_count,
                f,
                c,
                wave_length,
            } => write!(
                fmt,
                "Orcaella Async ({} leaders/round, f={f}, c={c}, wave length {})",
                leader_count, wave_length
            ),
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
            Self::Hydrozoan {
                leader_count,
                f,
                c,
                k,
            } => {
                write!(
                    fmt,
                    "Hydrozoan ({} leaders/round, f={f}, c={c}, k={k})",
                    leader_count
                )
            }
        }
    }
}

impl fmt::Debug for ConsensusProtocol {
    /// Compact, filesystem-friendly identifier (lowercase name plus the
    /// distinguishing parameters), used in result filenames.
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CordialMinersPartiallySynchronous => write!(fmt, "cordial-miners-ps"),
            Self::CordialMinersAsynchronous => write!(fmt, "cordial-miners-async"),
            Self::Mysticeti { leader_count } => write!(fmt, "mysticeti-l{leader_count}"),
            Self::BlueBottlePartiallySynchronous { leader_count } => {
                write!(fmt, "blue-bottle-ps-l{leader_count}")
            }
            Self::BlueBottleAsynchronous { leader_count } => {
                write!(fmt, "blue-bottle-async-l{leader_count}")
            }
            Self::Orcaella { leader_count, f, c } => {
                write!(fmt, "orcaella-l{leader_count}-f{f}-c{c}")
            }
            Self::OrcaellaAsynchronous {
                leader_count,
                f,
                c,
                wave_length,
            } => write!(
                fmt,
                "orcaella-async-l{leader_count}-f{f}-c{c}-w{wave_length}"
            ),
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => write!(fmt, "mahi-mahi-l{leader_count}-w{wave_length}"),
            Self::NemoNemo { leader_count } => write!(fmt, "nemo-nemo-l{leader_count}"),
            Self::Hydrozoan {
                leader_count,
                f,
                c,
                k,
            } => write!(fmt, "hydrozoan-l{leader_count}-f{f}-c{c}-k{k}"),
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
            | Self::BlueBottlePartiallySynchronous { leader_count }
            | Self::BlueBottleAsynchronous { leader_count }
            | Self::Orcaella { leader_count, .. }
            | Self::OrcaellaAsynchronous { leader_count, .. }
            | Self::MahiMahi { leader_count, .. }
            | Self::NemoNemo { leader_count }
            | Self::Hydrozoan { leader_count, .. } => Some(*leader_count),
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
            Self::BlueBottlePartiallySynchronous { leader_count } => {
                Protocol::blue_bottle_partially_synchronous(total_stake, leader_count)
            }
            Self::BlueBottleAsynchronous { leader_count } => {
                Protocol::blue_bottle_asynchronous(total_stake, leader_count)
            }
            Self::Orcaella { leader_count, f, c } => {
                Protocol::orcaella(total_stake, f, c, leader_count)?
            }
            Self::OrcaellaAsynchronous {
                leader_count,
                f,
                c,
                wave_length,
            } => Protocol::orcaella_asynchronous(total_stake, f, c, leader_count, wave_length)?,
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => Protocol::mahi_mahi(total_stake, leader_count, wave_length)?,
            Self::NemoNemo { leader_count } => Protocol::nemo_nemo(total_stake, leader_count),
            Self::Hydrozoan {
                leader_count,
                f,
                c,
                k,
            } => Protocol::hydrozoan(total_stake, f, c, k, leader_count)?,
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
            variants.push(Self::BlueBottlePartiallySynchronous { leader_count });
            variants.push(Self::BlueBottleAsynchronous { leader_count });
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
            variants.push(Self::OrcaellaAsynchronous {
                leader_count,
                f: 0,
                c: 1,
                wave_length: 3,
            });
            if n >= 6 {
                variants.push(Self::OrcaellaAsynchronous {
                    leader_count,
                    f: 1,
                    c: 0,
                    wave_length: 3,
                });
            }
            if n >= 9 {
                variants.push(Self::OrcaellaAsynchronous {
                    leader_count,
                    f: 1,
                    c: 1,
                    wave_length: 3,
                });
                variants.push(Self::OrcaellaAsynchronous {
                    leader_count,
                    f: 1,
                    c: 1,
                    wave_length: 4,
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
            variants.push(Self::Hydrozoan {
                leader_count,
                f: 1,
                c: 0,
                k: 0,
            });
            variants.push(Self::Hydrozoan {
                leader_count,
                f: 0,
                c: 1,
                k: 1,
            });
            if n >= 20 {
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 6,
                    c: 0,
                    k: 1,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 0,
                    c: 9,
                    k: 1,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 3,
                    c: 4,
                    k: 2,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 4,
                    c: 1,
                    k: 5,
                });
            }
        }
        variants.push(Self::CordialMinersPartiallySynchronous);
        variants.push(Self::CordialMinersAsynchronous);
        variants
    }

    /// Fast-path protocols at the baseline matrix configuration, for the
    /// fast-path-specific scenario tests.
    pub fn all_fast_path_for_test(n: usize, leader_counts: &[usize]) -> Vec<Self> {
        let mut variants = Vec::new();
        for &l in leader_counts {
            let leader_count = NonZeroUsize::new(l).expect("leader_count must be non-zero");
            variants.push(Self::Hydrozoan {
                leader_count,
                f: 1,
                c: 0,
                k: 0,
            });
            variants.push(Self::Hydrozoan {
                leader_count,
                f: 0,
                c: 1,
                k: 1,
            });
            if n >= 20 {
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 6,
                    c: 0,
                    k: 1,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 0,
                    c: 9,
                    k: 1,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 3,
                    c: 4,
                    k: 2,
                });
                variants.push(Self::Hydrozoan {
                    leader_count,
                    f: 4,
                    c: 1,
                    k: 5,
                });
            }
        }
        variants
    }
}

/// Errors that can arise when building a [`Protocol`] from a [`ConsensusProtocol`].
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("{protocol} fault bound violated: requires n >= {min_n}, got n = {n}")]
    FaultBoundViolated {
        protocol: &'static str,
        n: Stake,
        min_n: Stake,
    },
    #[error(
        "{protocol} requires wave_length in {{{}, {}}}, got {wave_length}",
        allowed[0],
        allowed[1]
    )]
    InvalidWaveLength {
        protocol: &'static str,
        wave_length: RoundNumber,
        allowed: [RoundNumber; 2],
    },
    #[error("leader_count ({leader_count}) exceeds committee size ({committee_size})")]
    LeaderCountExceedsCommittee {
        leader_count: NonZeroUsize,
        committee_size: usize,
    },
}

impl ProtocolError {
    fn check_wave_length(
        protocol: &'static str,
        wave_length: RoundNumber,
        allowed: [RoundNumber; 2],
    ) -> Result<(), Self> {
        if allowed.contains(&wave_length) {
            return Ok(());
        }
        Err(Self::InvalidWaveLength {
            protocol,
            wave_length,
            allowed,
        })
    }
}

/// Optimistic fast-path parameters for dual-path protocols.
#[derive(Clone, Copy)]
pub struct FastPath {
    /// The quorum of votes at the voting round to fast-commit a leader.
    pub commit_quorum: Stake,
    /// The quorum of anchor-linked votes to indirectly commit a leader
    /// (second rung of the graded indirect rule).
    pub weak_indirect_quorum: Stake,
}

/// Protocol-specific parameters for the consensus committer.
pub struct Protocol {
    /// The quorum threshold to directly commit a leader: the number of
    /// certificates required at the decision round (the outer threshold).
    pub direct_commit_quorum: Stake,
    /// The quorum threshold to directly skip a leader.
    pub direct_skip_quorum: Stake,
    /// The quorum of votes a decision block must reference to count as a
    /// certificate (the inner threshold).
    pub certificate_quorum: Stake,
    /// The quorum governing round advancement and block validity (parents /
    /// availability). Historically implicit in `direct_commit_quorum`, with
    /// which it coincides for all single-tier protocols.
    pub quorum_threshold: Stake,
    /// Optimistic fast-path parameters; `None` for single-tier protocols.
    pub fast_path: Option<FastPath>,
    /// The stake of anchor-linked support required to indirectly decide a leader.
    pub anchor_link_size: Stake,
    /// The number of rounds to commit a leader.
    pub wave_length: RoundNumber,
    /// Whether decision-round votes are themselves the certificates (no certify
    /// round); forced by geometry at `wave_length == 2`.
    pub merged_certificates: bool,
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
