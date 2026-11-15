// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Union certificates: a leader is directly committed when the votes held by the decision-round
//! blocks reach a quorum even after removing every vote that a later anchor could miss, instead
//! of requiring one block that references a quorum of votes.
//!
//! A validator holds decision-round blocks of total stake `M >= quorum_threshold`. With total
//! stake `N` and `F = N - quorum_threshold`, any later anchor shares at least `M - 2F` stake of
//! non-equivocated decision-round blocks with them: it misses at most `F` of the held stake and
//! Byzantine authors may have equivocated on at most `F` more. The anchor therefore sees every
//! vote that those shared blocks reference.
//!
//! The exact rule asks that every set of held blocks with at least `M - 2F` stake references votes
//! of at least a quorum of stake. That is a search over subsets. A voter is missed by such a set
//! only if every block of the set skips that voter, so any two voters that are missed together
//! are skipped by blocks of at least `M - 2F` stake in common. The stake of the largest group of
//! voters that are pairwise skipped together bounds the missed votes, which takes `O(n^2)` mask
//! operations. The bound can be loose, so this commits only when the exact rule does, and
//! sometimes later.

use dag::committee::Stake;

/// Authorities and blocks are indexed in `u64` masks.
pub(crate) const MAX_AUTHORITIES: usize = 64;

/// Whether the decision-round blocks of `authors` (one block per author, `vote_masks[i]` holding
/// bit `v` when block `i` references a vote of authority `v`) directly commit the leader.
pub(crate) fn union_direct_commit(
    stakes: &[Stake],
    quorum_threshold: Stake,
    certificate_quorum: Stake,
    authors: &[usize],
    vote_masks: &[u64],
) -> bool {
    commits_with_shared_stake_bonus(
        stakes,
        quorum_threshold,
        certificate_quorum,
        authors,
        vote_masks,
        0,
    )
}

/// `shared_stake_bonus` raises the shared stake that a later anchor is assumed to keep; it is
/// zero in production and only non-zero to check that the tests notice an unsound rule.
fn commits_with_shared_stake_bonus(
    stakes: &[Stake],
    quorum_threshold: Stake,
    certificate_quorum: Stake,
    authors: &[usize],
    vote_masks: &[u64],
    shared_stake_bonus: u128,
) -> bool {
    assert_eq!(authors.len(), vote_masks.len());
    assert!(
        stakes.len() <= MAX_AUTHORITIES,
        "union certificates need at most {MAX_AUTHORITIES} authorities"
    );

    let total: u128 = stakes.iter().map(|&stake| stake as u128).sum();
    let faults = total.saturating_sub(quorum_threshold as u128);
    let held: u128 = authors.iter().map(|&author| stakes[author] as u128).sum();
    if held < quorum_threshold as u128 {
        return false;
    }
    let Some(shared) = held.checked_sub(2 * faults).filter(|&shared| shared > 0) else {
        return false;
    };
    let shared = shared + shared_stake_bonus;

    let stake_of_voters = |mask: u64| -> u128 {
        (0..stakes.len())
            .filter(|&voter| mask >> voter & 1 == 1)
            .map(|voter| stakes[voter] as u128)
            .sum()
    };
    let stake_of_blocks = |mask: u64| -> u128 {
        (0..authors.len())
            .filter(|&block| mask >> block & 1 == 1)
            .map(|block| stakes[authors[block]] as u128)
            .sum()
    };

    let all_votes = vote_masks.iter().fold(0, |all, &mask| all | mask);
    let vote_stake = stake_of_voters(all_votes);
    if vote_stake < certificate_quorum as u128 {
        return false;
    }

    // The blocks that do not reference the vote of each voter, kept only for the voters that
    // blocks of at least `shared` stake can all skip.
    let skipped_together: Vec<(usize, u64)> = (0..stakes.len())
        .filter(|&voter| all_votes >> voter & 1 == 1)
        .map(|voter| {
            let skipping = vote_masks
                .iter()
                .enumerate()
                .filter(|&(_, &mask)| mask >> voter & 1 == 0)
                .fold(0u64, |all, (block, _)| all | 1 << block);
            (voter, skipping)
        })
        .filter(|&(_, skipping)| stake_of_blocks(skipping) >= shared)
        .collect();

    let missed = skipped_together
        .iter()
        .map(|&(_, skipping)| {
            skipped_together
                .iter()
                .filter(|&&(_, other)| stake_of_blocks(skipping & other) >= shared)
                .map(|&(voter, _)| stakes[voter] as u128)
                .sum::<u128>()
        })
        .max()
        .unwrap_or(0);

    vote_stake - missed >= certificate_quorum as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator, so the tests need no extra dependency.
    struct Xorshift(u64);

    impl Xorshift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    /// The rule without the pair shortcut: every set of held blocks with at least `M - 2F` stake
    /// must reference votes of a quorum.
    fn exact(
        stakes: &[Stake],
        quorum_threshold: Stake,
        certificate_quorum: Stake,
        authors: &[usize],
        vote_masks: &[u64],
    ) -> bool {
        let total: u128 = stakes.iter().map(|&stake| stake as u128).sum();
        let faults = total - quorum_threshold as u128;
        let held: u128 = authors.iter().map(|&author| stakes[author] as u128).sum();
        if held < quorum_threshold as u128 || held <= 2 * faults {
            return false;
        }
        let shared = held - 2 * faults;
        (1u64..1 << authors.len()).all(|subset| {
            let blocks = || (0..authors.len()).filter(|&block| subset >> block & 1 == 1);
            let stake: u128 = blocks().map(|block| stakes[authors[block]] as u128).sum();
            if stake < shared {
                return true;
            }
            let votes = blocks().fold(0, |all, block| all | vote_masks[block]);
            let covered: u128 = (0..stakes.len())
                .filter(|&voter| votes >> voter & 1 == 1)
                .map(|voter| stakes[voter] as u128)
                .sum();
            covered >= certificate_quorum as u128
        })
    }

    /// Random held blocks of a committee with random stakes, each referencing the vote of a
    /// random set of authorities that holds a quorum of stake.
    fn random_instance(rng: &mut Xorshift) -> (Vec<Stake>, Stake, Vec<usize>, Vec<u64>) {
        let size = 4 + rng.below(7) as usize;
        let stakes: Vec<Stake> = if rng.below(2) == 0 {
            vec![1; size]
        } else {
            (0..size).map(|_| 10 + rng.below(91)).collect()
        };
        let total: Stake = stakes.iter().sum();
        let quorum = 2 * total / 3 + 1;
        let voters: Vec<bool> = {
            let rate = [50, 70, 85, 100][rng.below(4) as usize];
            (0..size).map(|_| rng.below(100) < rate).collect()
        };
        let mut order: Vec<usize> = (0..size).collect();
        for i in (1..size).rev() {
            order.swap(i, rng.below(i as u64 + 1) as usize);
        }
        let mut authors = vec![];
        let mut held = 0;
        for &author in &order {
            authors.push(author);
            held += stakes[author];
            if held >= quorum && rng.below(2) == 0 {
                break;
            }
        }
        let masks = authors
            .iter()
            .map(|_| {
                let mut order: Vec<usize> = (0..size).collect();
                for i in (1..size).rev() {
                    order.swap(i, rng.below(i as u64 + 1) as usize);
                }
                let mut referenced = 0;
                let mut stake = 0;
                for voter in order {
                    if stake >= quorum {
                        break;
                    }
                    stake += stakes[voter];
                    if voters[voter] {
                        referenced |= 1 << voter;
                    }
                }
                referenced
            })
            .collect();
        (stakes, quorum, authors, masks)
    }

    /// With 50 authorities every block references a random quorum (34) of the voters, as under
    /// minimum-quorum references. With exactly 34 voters every vote is needed and a later anchor
    /// can miss one, so the leader does not commit. With 35 or more the anchor can miss one and
    /// still see a quorum, so it commits, although almost no block references 34 votes itself.
    #[test]
    fn commits_at_fifty_authorities_once_a_vote_can_be_lost() {
        let mut rng = Xorshift(0xD1B5_4A32_D192_ED03);
        let stakes = vec![1; 50];
        let authors: Vec<usize> = (0..50).collect();
        for (voters, commits) in [(34, false), (35, true), (42, true), (50, true)] {
            for _ in 0..50 {
                let masks: Vec<u64> = authors
                    .iter()
                    .map(|_| {
                        let mut order: Vec<usize> = (0..50).collect();
                        for i in (1..50).rev() {
                            order.swap(i, rng.below(i as u64 + 1) as usize);
                        }
                        order[..34]
                            .iter()
                            .filter(|&&voter| voter < voters)
                            .fold(0u64, |all, &voter| all | 1 << voter)
                    })
                    .collect();
                assert_eq!(
                    union_direct_commit(&stakes, 34, 34, &authors, &masks),
                    commits,
                    "{voters} voters"
                );
            }
        }
    }

    #[test]
    fn commits_when_every_block_references_a_quorum_of_votes() {
        let stakes = [1, 1, 1, 1];
        let masks = [0b0111, 0b1011, 0b1101, 0b1110];
        assert!(union_direct_commit(&stakes, 3, 3, &[0, 1, 2, 3], &masks));
    }

    #[test]
    fn commits_with_votes_spread_over_the_blocks() {
        // No block references three votes, but any two of the four blocks together do.
        let stakes = [1, 1, 1, 1];
        let masks = [0b0011, 0b1100, 0b0101, 0b1010];
        assert!(union_direct_commit(&stakes, 3, 3, &[0, 1, 2, 3], &masks));
        assert!(!exact(&stakes, 3, 3, &[0, 1, 2], &masks[..3]));
    }

    #[test]
    fn does_not_commit_without_a_quorum_of_votes() {
        let stakes = [1, 1, 1, 1];
        let masks = [0b0011, 0b0011, 0b0011, 0b0011];
        assert!(!union_direct_commit(&stakes, 3, 3, &[0, 1, 2, 3], &masks));
    }

    #[test]
    fn commits_when_one_voter_can_be_missed_but_the_rest_is_a_quorum() {
        // Two blocks skip the vote of authority 3: an anchor that shares only those two still
        // sees the three other votes.
        let stakes = [1, 1, 1, 1];
        let masks = [0b0111, 0b0111, 0b1111, 0b1111];
        assert!(union_direct_commit(&stakes, 3, 3, &[0, 1, 2, 3], &masks));
    }

    #[test]
    fn does_not_commit_when_two_voters_can_be_missed() {
        // Two blocks skip the votes of authorities 2 and 3: an anchor that shares only those
        // two sees two votes.
        let stakes = [1, 1, 1, 1];
        let masks = [0b0011, 0b0011, 0b1111, 0b1111];
        assert!(!union_direct_commit(&stakes, 3, 3, &[0, 1, 2, 3], &masks));
    }

    #[test]
    fn does_not_commit_below_a_quorum_of_held_blocks() {
        let stakes = [1, 1, 1, 1];
        assert!(!union_direct_commit(
            &stakes,
            3,
            3,
            &[0, 1],
            &[0b1111, 0b1111]
        ));
    }

    #[test]
    fn weighs_blocks_and_votes_by_stake() {
        let stakes = [4, 1, 1, 1, 1];
        let authors = [0, 1, 2, 3, 4];
        let all = 0b11111;
        assert!(union_direct_commit(&stakes, 5, 5, &authors, &[all; 5]));
        // Two light blocks skip the heavy voter: an anchor that shares only them sees stake 4.
        let masks = [all, 0b11110, 0b11110, all, all];
        assert!(!union_direct_commit(&stakes, 5, 5, &authors, &masks));
        // The heavy block skips two light voters: the rest still holds stake 6.
        let masks = [0b11001, all, all, all, all];
        assert!(union_direct_commit(&stakes, 5, 5, &authors, &masks));
    }

    /// The pair shortcut commits only when the exact rule commits, for equal and unequal stakes.
    #[test]
    fn never_commits_when_the_exact_rule_does_not() {
        let mut rng = Xorshift(0x9E37_79B9_7F4A_7C15);
        let mut commits = 0;
        for _ in 0..4000 {
            let (stakes, quorum, authors, masks) = random_instance(&mut rng);
            if union_direct_commit(&stakes, quorum, quorum, &authors, &masks) {
                commits += 1;
                assert!(
                    exact(&stakes, quorum, quorum, &authors, &masks),
                    "{stakes:?} {authors:?} {masks:?}"
                );
            }
        }
        assert!(commits > 100, "too few commits to be meaningful: {commits}");
    }

    /// Assuming the anchor shares more stake than the bound guarantees must be caught.
    #[test]
    fn notices_an_unsound_shared_stake() {
        let mut rng = Xorshift(0x2545_F491_4F6C_DD1D);
        let unsound = (0..4000)
            .map(|_| random_instance(&mut rng))
            .filter(|(stakes, quorum, authors, masks)| {
                commits_with_shared_stake_bonus(stakes, *quorum, *quorum, authors, masks, 1)
                    && !exact(stakes, *quorum, *quorum, authors, masks)
            })
            .count();
        assert!(unsound > 0);
    }
}
