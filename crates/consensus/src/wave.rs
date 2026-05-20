// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::block::RoundNumber;

pub(crate) type WaveNumber = RoundNumber;

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round,
/// at least one voting round, and one decision round. This type owns the rounding
/// arithmetic that maps between rounds and waves for a given `(wave_length, round_offset)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Wave {
    wave_length: RoundNumber,
    round_offset: RoundNumber,
}

impl Wave {
    pub(crate) fn new(wave_length: RoundNumber, round_offset: RoundNumber) -> Self {
        debug_assert!(wave_length > 0, "wave_length must be positive");
        debug_assert!(
            round_offset < wave_length,
            "round_offset must be < wave_length",
        );
        Self {
            wave_length,
            round_offset,
        }
    }

    #[inline]
    pub(crate) fn length(&self) -> RoundNumber {
        self.wave_length
    }

    #[inline]
    pub(crate) fn round_offset(&self) -> RoundNumber {
        self.round_offset
    }

    /// Return the wave in which the specified round belongs.
    #[inline]
    pub(crate) fn number(&self, round: RoundNumber) -> WaveNumber {
        round.saturating_sub(self.round_offset) / self.wave_length
    }

    /// Return the leader round of the specified wave number.
    #[inline]
    pub(crate) fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.wave_length + self.round_offset
    }

    /// Return the decision round of the specified wave.
    #[inline]
    pub(crate) fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        self.leader_round(wave) + self.wave_length - 1
    }

    /// Return the voting round of the specified wave.
    #[inline]
    pub(crate) fn voting_round(&self, wave: WaveNumber) -> RoundNumber {
        let leader_round = self.leader_round(wave);
        let decision_round = self.decision_round(wave);
        (leader_round + 1).max(decision_round - 1)
    }

    /// True iff `round` is the leader round of some wave.
    #[inline]
    pub(crate) fn is_leader_round(&self, round: RoundNumber) -> bool {
        self.leader_round(self.number(round)) == round
    }
}

#[cfg(test)]
mod tests {
    use crate::wave::Wave;

    /// Iterate over the meaningful `(wave_length, round_offset)` configurations.
    fn configs() -> impl Iterator<Item = (u64, u64)> {
        (2u64..=5).flat_map(|wave_length| (0..wave_length).map(move |offset| (wave_length, offset)))
    }

    #[test]
    fn leader_round_inverse_of_number() {
        for (wave_length, round_offset) in configs() {
            let wave = Wave::new(wave_length, round_offset);
            for wave_number in 0..4 {
                let leader_round = wave.leader_round(wave_number);
                assert_eq!(
                    wave.number(leader_round),
                    wave_number,
                    "inverse failed for wl={wave_length}, ro={round_offset}, w={wave_number}",
                );
            }
        }
    }

    #[test]
    fn decision_round_relation() {
        for (wave_length, round_offset) in configs() {
            let wave = Wave::new(wave_length, round_offset);
            for wave_number in 0..4 {
                assert_eq!(
                    wave.decision_round(wave_number),
                    wave.leader_round(wave_number) + wave_length - 1,
                );
            }
        }
    }

    #[test]
    fn voting_round_collapses_when_wl_is_two() {
        for round_offset in 0..2 {
            let wave = Wave::new(2, round_offset);
            for wave_number in 0..3 {
                let leader_round = wave.leader_round(wave_number);
                let decision_round = wave.decision_round(wave_number);
                let voting_round = wave.voting_round(wave_number);
                assert_eq!(voting_round, leader_round + 1);
                assert_eq!(voting_round, decision_round);
            }
        }
    }

    #[test]
    fn is_leader_round_matches_modulo() {
        for (wave_length, round_offset) in configs() {
            let wave = Wave::new(wave_length, round_offset);
            for round in 0..20 {
                let expected = round >= round_offset && (round - round_offset) % wave_length == 0;
                assert_eq!(
                    wave.is_leader_round(round),
                    expected,
                    "is_leader_round({round}) mismatch for ({wave_length}, {round_offset})",
                );
            }
        }
    }

    #[test]
    fn table_driven_full_matrix() {
        // (wave_length, round_offset, round) -> (wave, leader_round, decision_round, voting_round)
        const TABLE: &[(u64, u64, u64, u64, u64, u64, u64)] = &[
            (2, 0, 0, 0, 0, 1, 1),
            (2, 0, 1, 0, 0, 1, 1),
            (2, 0, 2, 1, 2, 3, 3),
            (2, 0, 5, 2, 4, 5, 5),
            (2, 1, 0, 0, 1, 2, 2),
            (2, 1, 1, 0, 1, 2, 2),
            (2, 1, 3, 1, 3, 4, 4),
            (3, 0, 0, 0, 0, 2, 1),
            (3, 0, 2, 0, 0, 2, 1),
            (3, 0, 3, 1, 3, 5, 4),
            (3, 0, 6, 2, 6, 8, 7),
            (3, 1, 0, 0, 1, 3, 2),
            (3, 1, 1, 0, 1, 3, 2),
            (3, 1, 4, 1, 4, 6, 5),
            (3, 2, 0, 0, 2, 4, 3),
            (3, 2, 1, 0, 2, 4, 3),
            (3, 2, 2, 0, 2, 4, 3),
            (3, 2, 5, 1, 5, 7, 6),
            (4, 0, 0, 0, 0, 3, 2),
            (4, 0, 3, 0, 0, 3, 2),
            (4, 0, 4, 1, 4, 7, 6),
            (4, 0, 8, 2, 8, 11, 10),
            (4, 1, 0, 0, 1, 4, 3),
            (4, 1, 5, 1, 5, 8, 7),
            (4, 2, 1, 0, 2, 5, 4),
            (4, 3, 0, 0, 3, 6, 5),
            (4, 3, 7, 1, 7, 10, 9),
            (5, 0, 0, 0, 0, 4, 3),
            (5, 0, 4, 0, 0, 4, 3),
            (5, 0, 5, 1, 5, 9, 8),
            (5, 0, 10, 2, 10, 14, 13),
            (5, 1, 0, 0, 1, 5, 4),
            (5, 2, 1, 0, 2, 6, 5),
            (5, 3, 2, 0, 3, 7, 6),
            (5, 4, 3, 0, 4, 8, 7),
            (5, 4, 9, 1, 9, 13, 12),
        ];

        for &(
            wave_length,
            round_offset,
            round,
            expected_wave,
            expected_leader,
            expected_decision,
            expected_voting,
        ) in TABLE
        {
            let wave = Wave::new(wave_length, round_offset);
            let wave_number = wave.number(round);
            assert_eq!(
                wave_number, expected_wave,
                "number({round}) for ({wave_length}, {round_offset})",
            );
            assert_eq!(
                wave.leader_round(wave_number),
                expected_leader,
                "leader_round for ({wave_length}, {round_offset}, {round})",
            );
            assert_eq!(
                wave.decision_round(wave_number),
                expected_decision,
                "decision_round for ({wave_length}, {round_offset}, {round})",
            );
            assert_eq!(
                wave.voting_round(wave_number),
                expected_voting,
                "voting_round for ({wave_length}, {round_offset}, {round})",
            );
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "wave_length must be positive")]
    fn new_panics_on_zero_wave_length() {
        Wave::new(0, 0);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "round_offset must be < wave_length")]
    fn new_panics_on_offset_ge_wave_length() {
        Wave::new(3, 3);
    }
}
