// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use dag::{
    block::RoundNumber,
    committee::{Committee, Stake},
    core::RETAIN_BELOW_COMMIT_ROUNDS,
    sync::net_sync::QuorumTimeoutRounds,
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
    DagHydrangea {
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
        /// Byzantine-fault stake to tolerate.
        f: Stake,
        /// Crash-only fault stake to tolerate (on top of `f`).
        c: Stake,
        /// Tunable slack widening the fast path; requires `n >= 3f + 2c + k + 1`.
        k: Stake,
    },
    Steelhead {
        pair: SteelheadPair,
        /// Every `period`-th round is an async slot; `None` means never (pure sync rule).
        /// Mutually exclusive with `adaptive`, which starts at `max_period` and adapts.
        #[serde(default)]
        period: Option<NonZeroU64>,
        /// Async-rule wave length: 4 or 5 for Mysticeti/Mahi-Mahi, exactly 3 for Blue Bottle.
        async_wave_length: RoundNumber,
        /// Adapt the period by counterfactual replay of the committed window.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adaptive: Option<AdaptiveConfig>,
        /// Every `canary`-th async round waits for the round-robin cohort, so
        /// the DAG keeps recording sync viability at any period; `null`
        /// disables the wait (pure quorum pacing on async rounds). Values
        /// above 1 under-sample the replay's sync evidence (conservative).
        #[serde(default = "defaults::default_canary")]
        canary: Option<NonZeroU64>,
        #[serde(default = "defaults::default_leader_count")]
        leader_count: NonZeroUsize,
    },
}

/// Adaptive-period parameters: every `interval` rounds, replay the committed
/// window under each candidate period (powers of two up to `max_period`) and
/// adopt the argmin, with `epsilon_percent` hysteresis.
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct AdaptiveConfig {
    pub interval: u64,
    pub max_period: NonZeroU64,
    pub epsilon_percent: u8,
}

/// The pair of base rules a Steelhead instantiation composes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SteelheadPair {
    MysticetiMahiMahi,
    BlueBottle,
}

mod defaults {
    use std::num::{NonZeroU64, NonZeroUsize};

    use dag::block::RoundNumber;
    use serde::{Deserialize, Deserializer, de::Error as _};

    pub fn default_leader_count() -> NonZeroUsize {
        NonZeroUsize::new(2).unwrap()
    }

    pub fn default_canary() -> Option<NonZeroU64> {
        NonZeroU64::new(1)
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
            Self::DagHydrangea {
                leader_count,
                f,
                c,
                k,
            } => {
                write!(
                    fmt,
                    "DagHydrangea ({} leaders/round, f={f}, c={c}, k={k})",
                    leader_count
                )
            }
            Self::Steelhead {
                pair,
                period,
                async_wave_length,
                adaptive,
                canary,
                leader_count,
            } => {
                let pair_name = match pair {
                    SteelheadPair::MysticetiMahiMahi => "Mysticeti+Mahi-Mahi",
                    SteelheadPair::BlueBottle => "Blue Bottle",
                };
                match (adaptive, period) {
                    (Some(adaptive), _) => write!(
                        fmt,
                        "Steelhead {pair_name} (adaptive to {}, interval {}, async wave \
                        {async_wave_length}, {leader_count} leaders/round)",
                        adaptive.max_period, adaptive.interval
                    )?,
                    (None, Some(period)) => write!(
                        fmt,
                        "Steelhead {pair_name} (period {period}, async wave {async_wave_length}, \
                        {leader_count} leaders/round)"
                    )?,
                    (None, None) => write!(
                        fmt,
                        "Steelhead {pair_name} (period ∞, async wave {async_wave_length}, \
                        {leader_count} leaders/round)"
                    )?,
                }
                match canary {
                    Some(canary) if canary.get() == 1 => Ok(()),
                    Some(canary) => write!(fmt, " (canary {canary})"),
                    None => write!(fmt, " (no canary)"),
                }
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
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => write!(fmt, "mahi-mahi-l{leader_count}-w{wave_length}"),
            Self::NemoNemo { leader_count } => write!(fmt, "nemo-nemo-l{leader_count}"),
            Self::DagHydrangea {
                leader_count,
                f,
                c,
                k,
            } => write!(fmt, "dag-hydrangea-l{leader_count}-f{f}-c{c}-k{k}"),
            Self::Steelhead {
                pair,
                period,
                async_wave_length,
                adaptive,
                canary,
                leader_count,
            } => {
                let pair_tag = match pair {
                    SteelheadPair::MysticetiMahiMahi => "mm",
                    SteelheadPair::BlueBottle => "bb",
                };
                match (adaptive, period) {
                    (Some(adaptive), _) => {
                        let (max_period, interval) = (adaptive.max_period, adaptive.interval);
                        write!(
                            fmt,
                            "steelhead-{pair_tag}-adaptive-m{max_period}-i{interval}\
                            -w{async_wave_length}-l{leader_count}"
                        )?;
                    }
                    (None, Some(period)) => write!(
                        fmt,
                        "steelhead-{pair_tag}-p{period}-w{async_wave_length}-l{leader_count}"
                    )?,
                    (None, None) => write!(
                        fmt,
                        "steelhead-{pair_tag}-pinf-w{async_wave_length}-l{leader_count}"
                    )?,
                }
                // The default (canary 1) keeps pre-existing tags stable.
                match canary {
                    Some(canary) if canary.get() == 1 => Ok(()),
                    Some(canary) => write!(fmt, "-c{canary}"),
                    None => write!(fmt, "-cinf"),
                }
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
            | Self::BlueBottlePartiallySynchronous { leader_count }
            | Self::BlueBottleAsynchronous { leader_count }
            | Self::Orcaella { leader_count, .. }
            | Self::MahiMahi { leader_count, .. }
            | Self::NemoNemo { leader_count }
            | Self::DagHydrangea { leader_count, .. }
            | Self::Steelhead { leader_count, .. } => Some(*leader_count),
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
            Self::MahiMahi {
                leader_count,
                wave_length,
            } => Protocol::mahi_mahi(total_stake, leader_count, wave_length)?,
            Self::NemoNemo { leader_count } => Protocol::nemo_nemo(total_stake, leader_count),
            Self::DagHydrangea {
                leader_count,
                f,
                c,
                k,
            } => Protocol::dag_hydrangea(total_stake, f, c, k, leader_count)?,
            Self::Steelhead {
                pair,
                period,
                async_wave_length,
                adaptive,
                canary,
                leader_count,
            } => Protocol::steelhead(
                total_stake,
                pair,
                period,
                async_wave_length,
                adaptive,
                canary,
                leader_count,
            )?,
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
            variants.push(Self::MahiMahi {
                leader_count,
                wave_length: 4,
            });
            variants.push(Self::MahiMahi {
                leader_count,
                wave_length: 5,
            });
            // Degenerate-period Steelhead runs the pure base rules; finite
            // periods are covered by the dedicated steelhead tests (an async
            // slot between the first slot and the target can out-live the
            // generic scenario constructions' DAG depth).
            for pure_period in [None, NonZeroU64::new(1)] {
                variants.push(Self::Steelhead {
                    pair: SteelheadPair::MysticetiMahiMahi,
                    period: pure_period,
                    async_wave_length: 5,
                    adaptive: None,
                    canary: defaults::default_canary(),
                    leader_count,
                });
                variants.push(Self::Steelhead {
                    pair: SteelheadPair::BlueBottle,
                    period: pure_period,
                    async_wave_length: 3,
                    adaptive: None,
                    canary: defaults::default_canary(),
                    leader_count,
                });
            }
            variants.push(Self::DagHydrangea {
                leader_count,
                f: 1,
                c: 0,
                k: 0,
            });
            variants.push(Self::DagHydrangea {
                leader_count,
                f: 0,
                c: 1,
                k: 1,
            });
            if n >= 20 {
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 6,
                    c: 0,
                    k: 1,
                });
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 0,
                    c: 9,
                    k: 1,
                });
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 3,
                    c: 4,
                    k: 2,
                });
                variants.push(Self::DagHydrangea {
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
            variants.push(Self::DagHydrangea {
                leader_count,
                f: 1,
                c: 0,
                k: 0,
            });
            variants.push(Self::DagHydrangea {
                leader_count,
                f: 0,
                c: 1,
                k: 1,
            });
            if n >= 20 {
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 6,
                    c: 0,
                    k: 1,
                });
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 0,
                    c: 9,
                    k: 1,
                });
                variants.push(Self::DagHydrangea {
                    leader_count,
                    f: 3,
                    c: 4,
                    k: 2,
                });
                variants.push(Self::DagHydrangea {
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
    #[error("Mahi-Mahi requires wave_length in {{4, 5}}, got {wave_length}")]
    MahiMahiInvalidWaveLength { wave_length: RoundNumber },
    #[error(
        "Steelhead async_wave_length invalid for {pair:?}: got {async_wave_length} \
        (Mysticeti/Mahi-Mahi requires 4 or 5, Blue Bottle requires 3)"
    )]
    SteelheadInvalidAsyncWaveLength {
        pair: SteelheadPair,
        async_wave_length: RoundNumber,
    },
    #[error("Steelhead adaptive config invalid: {reason}")]
    SteelheadInvalidAdaptive { reason: &'static str },
    #[error("leader_count ({leader_count}) exceeds committee size ({committee_size})")]
    LeaderCountExceedsCommittee {
        leader_count: NonZeroUsize,
        committee_size: usize,
    },
}

/// Steelhead's per-round wavelength schedule.
#[derive(Clone, Copy)]
pub struct SteelheadSchedule {
    /// Every `period`-th round is an async slot; `None` means never. With
    /// `adaptive` set, this is the initial period (`max_period`).
    pub period: Option<NonZeroU64>,
    pub sync_wave_length: RoundNumber,
    pub async_wave_length: RoundNumber,
    /// Adaptive-period parameters; `None` keeps the period static.
    pub adaptive: Option<AdaptiveConfig>,
    /// Async rounds that are multiples of `canary` keep the leader wait for
    /// the round-robin cohort; `None` disables it (pure quorum pacing).
    pub canary: Option<NonZeroU64>,
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
    /// Per-round wavelength schedule for Steelhead; `None` for all base protocols.
    pub steelhead: Option<SteelheadSchedule>,
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
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
            steelhead: None,
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
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 5,
            merged_certificates: false,
            steelhead: None,
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
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Blue Bottle (partially synchronous variant)
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn blue_bottle_partially_synchronous(
        total_stake: Stake,
        leader_count: NonZeroUsize,
    ) -> Self {
        let strong_quorum = 4 * total_stake / 5 + 1;
        let weak_quorum = 2 * total_stake / 5 + 1;
        Self {
            direct_commit_quorum: strong_quorum,
            direct_skip_quorum: strong_quorum,
            certificate_quorum: strong_quorum,
            quorum_threshold: strong_quorum,
            fast_path: None,
            anchor_link_size: weak_quorum,
            wave_length: 2,
            merged_certificates: true,
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
        }
    }

    /// Blue Bottle (asynchronous variant)
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn blue_bottle_asynchronous(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let strong_quorum = 4 * total_stake / 5 + 1;
        let weak_quorum = 2 * total_stake / 5 + 1;
        Self {
            direct_commit_quorum: strong_quorum,
            direct_skip_quorum: strong_quorum,
            certificate_quorum: strong_quorum,
            quorum_threshold: strong_quorum,
            fast_path: None,
            anchor_link_size: weak_quorum,
            wave_length: 3,
            merged_certificates: true,
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: false,
            require_crypto: true,
        }
    }

    /// Orcaella
    ///
    /// "Orcaella: Hybrid Fault Tolerance with Client-Selectable Finality Latency"
    /// <https://sonnino.com/papers/orcaella.pdf>
    pub fn orcaella(
        total_stake: Stake,
        f: Stake,
        c: Stake,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        let min_n = if f == 0 { 2 * c + 1 } else { 5 * f + 3 * c + 1 };
        if min_n > total_stake {
            return Err(ProtocolError::FaultBoundViolated {
                protocol: "Orcaella",
                n: total_stake,
                min_n,
            });
        }

        Ok(Self {
            direct_commit_quorum: total_stake - f - c,
            direct_skip_quorum: 4 * f + 2 * c + 1,
            certificate_quorum: total_stake - f - c,
            quorum_threshold: total_stake - f - c,
            fast_path: None,
            anchor_link_size: total_stake - 3 * f - 2 * c,
            wave_length: 2,
            merged_certificates: true,
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: f != 0,
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
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length,
            merged_certificates: false,
            steelhead: None,
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
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 2,
            merged_certificates: true,
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: false,
        }
    }

    /// DagHydrangea
    ///
    /// "Hydrangea: Optimistic Two-Round Partial Synchrony with Improved Fault Resilience"
    /// <https://eprint.iacr.org/2025/1112>
    pub fn dag_hydrangea(
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
                protocol: "DagHydrangea",
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
            steelhead: None,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: f != 0,
        })
    }

    /// Steelhead
    ///
    /// "Steelhead: Dual-Mode DAG Consensus Without Mode Switching"
    ///
    /// Composes the pair's sync and async rules over one DAG: every `period`-th
    /// round is an async slot decided at `async_wave_length`, the others sync
    /// slots at the pair's sync wave length. Both rules share the pair's quorums.
    pub fn steelhead(
        total_stake: Stake,
        pair: SteelheadPair,
        period: Option<NonZeroU64>,
        async_wave_length: RoundNumber,
        adaptive: Option<AdaptiveConfig>,
        canary: Option<NonZeroU64>,
        leader_count: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        let valid_async_wave = match pair {
            SteelheadPair::MysticetiMahiMahi => [4, 5].contains(&async_wave_length),
            SteelheadPair::BlueBottle => async_wave_length == 3,
        };
        if !valid_async_wave {
            return Err(ProtocolError::SteelheadInvalidAsyncWaveLength {
                pair,
                async_wave_length,
            });
        }
        let period = match adaptive {
            None => period,
            Some(adaptive) => {
                if period.is_some() {
                    return Err(ProtocolError::SteelheadInvalidAdaptive {
                        reason: "period must be omitted when adaptive is set",
                    });
                }
                if !adaptive.max_period.get().is_power_of_two() {
                    return Err(ProtocolError::SteelheadInvalidAdaptive {
                        reason: "max_period must be a power of two",
                    });
                }
                if adaptive.interval < 4 * adaptive.max_period.get() {
                    return Err(ProtocolError::SteelheadInvalidAdaptive {
                        reason: "interval must be at least 4 * max_period",
                    });
                }
                if adaptive.interval > RETAIN_BELOW_COMMIT_ROUNDS {
                    return Err(ProtocolError::SteelheadInvalidAdaptive {
                        reason: "interval must be at most the in-memory retention window \
                            (dag::core::RETAIN_BELOW_COMMIT_ROUNDS)",
                    });
                }
                if adaptive.epsilon_percent >= 100 {
                    return Err(ProtocolError::SteelheadInvalidAdaptive {
                        reason: "epsilon_percent must be below 100",
                    });
                }
                Some(adaptive.max_period)
            }
        };

        let mut protocol = match pair {
            SteelheadPair::MysticetiMahiMahi => Self::mysticeti(total_stake, leader_count),
            SteelheadPair::BlueBottle => {
                Self::blue_bottle_partially_synchronous(total_stake, leader_count)
            }
        };
        protocol.steelhead = Some(SteelheadSchedule {
            period,
            sync_wave_length: protocol.wave_length,
            async_wave_length,
            adaptive,
            canary,
        });
        Ok(protocol)
    }
}

/// Default cap on waiting for the leader block(s) of a leader-wait round.
pub const DEFAULT_LEADER_ROUND_TIMEOUT: Duration = Duration::from_secs(1);
/// Default cap on waiting for stragglers past the quorum on the other rounds.
pub const DEFAULT_QUORUM_ROUND_TIMEOUT: Duration = Duration::from_millis(75);

impl Protocol {
    /// Which rounds advance under the quorum cap instead of the leader cap:
    /// none for the partially synchronous protocols, all for the asynchronous
    /// ones, and Steelhead's async slots for a finite period.
    /// Round-timeout classification for the base protocols. Steelhead builds
    /// its own [`QuorumTimeoutRounds::Modal`] at the wiring sites (it needs
    /// the shared period cell).
    pub fn quorum_timeout_rounds(&self) -> QuorumTimeoutRounds {
        debug_assert!(self.steelhead.is_none(), "Steelhead classifies via Modal");
        if self.leader_wait {
            QuorumTimeoutRounds::None
        } else {
            QuorumTimeoutRounds::Every
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use crate::protocol::{ConsensusProtocol, Protocol, ProtocolError, SteelheadPair};

    /// Reference thresholds from the design note at the mixed configuration
    /// n = 20, f = 3, c = 4, k = 2 (p = 3), where the certificate, slow-commit,
    /// and pacemaker quorums are all distinct.
    #[test]
    fn dag_hydrangea_thresholds_mixed_configuration() {
        let protocol = Protocol::dag_hydrangea(20, 3, 4, 2, NonZeroUsize::new(1).unwrap()).unwrap();
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
    fn dag_hydrangea_quorums_coincide_at_zero_slack() {
        let protocol =
            Protocol::dag_hydrangea(99, 10, 34, 0, NonZeroUsize::new(1).unwrap()).unwrap();
        assert_eq!(protocol.certificate_quorum, 55);
        assert_eq!(protocol.direct_commit_quorum, 55);
        assert_eq!(protocol.quorum_threshold, 55);
    }

    /// The fault bound `n >= 3f + 2c + k + 1` is enforced.
    #[test]
    fn dag_hydrangea_fault_bound_rejected() {
        let result = Protocol::dag_hydrangea(4, 1, 0, 1, NonZeroUsize::new(1).unwrap());
        assert!(matches!(
            result,
            Err(ProtocolError::FaultBoundViolated {
                protocol: "DagHydrangea",
                n: 4,
                min_n: 5,
            })
        ));
    }

    /// The canary defaults to every round; an explicit null disables it.
    #[test]
    fn steelhead_canary_serde() {
        let base = "protocol: steelhead\npair: mysticeti-mahi-mahi\n\
            period: 4\nasync_wave_length: 5\n";
        let parse = |yaml: &str| -> ConsensusProtocol {
            serde_yaml::from_str(yaml).expect("valid steelhead config")
        };
        let canary_of = |spec: &ConsensusProtocol| match spec {
            ConsensusProtocol::Steelhead { canary, .. } => *canary,
            other => panic!("expected steelhead, got {other:?}"),
        };
        assert_eq!(canary_of(&parse(base)), NonZeroU64::new(1));
        assert_eq!(canary_of(&parse(&format!("{base}canary: null\n"))), None);
        assert_eq!(
            canary_of(&parse(&format!("{base}canary: 8\n"))),
            NonZeroU64::new(8)
        );
    }

    /// The default canary keeps pre-existing result-file tags stable; other
    /// values are distinguishable.
    #[test]
    fn steelhead_canary_debug_tags() {
        let spec = |canary| ConsensusProtocol::Steelhead {
            pair: SteelheadPair::MysticetiMahiMahi,
            period: NonZeroU64::new(4),
            async_wave_length: 5,
            adaptive: None,
            canary,
            leader_count: NonZeroUsize::new(2).unwrap(),
        };
        assert_eq!(
            format!("{:?}", spec(NonZeroU64::new(1))),
            "steelhead-mm-p4-w5-l2"
        );
        assert_eq!(
            format!("{:?}", spec(NonZeroU64::new(8))),
            "steelhead-mm-p4-w5-l2-c8"
        );
        assert_eq!(format!("{:?}", spec(None)), "steelhead-mm-p4-w5-l2-cinf");
    }
}
