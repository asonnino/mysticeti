// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Byzantine behaviour injected at the wire: an equivocating leader.

use consensus::leader::LeaderElector;
use dag::authority::Authority;
use dag::block::{Block, data::Data};
use dag::crypto::{BLOCK_DIGEST_SIZE, BlockDigest};
use dag::sync::network::NetworkMessage;

/// A Byzantine authority that, in its leader rounds, sends every peer its block and a twin
/// with a different digest; odd-indexed peers receive the twin first. The twin is also
/// reflected into the equivocator's own DAG so that honest blocks referencing it stay
/// causally complete for the equivocator, which otherwise stalls behind a missing parent.
pub(crate) struct Equivocator {
    authority: Authority,
    elector: LeaderElector,
    leader_count: u64,
}

impl Equivocator {
    /// The equivocator at committee `index`, leading with `leader_count` slots per round.
    pub(crate) fn new(index: usize, committee_size: usize, leader_count: usize) -> Self {
        Self {
            authority: Authority::from(index),
            elector: LeaderElector::new(committee_size),
            leader_count: leader_count as u64,
        }
    }

    /// Whether `block` is our own proposal in a round where we hold a leader slot.
    fn is_leader_block(&self, block: &Block) -> bool {
        block.author() == self.authority
            && (0..self.leader_count)
                .any(|offset| self.elector.elect_leader(block.round() + offset) == self.authority)
    }

    /// The same block under a different digest (last byte flipped).
    fn twin(block: &Data<Block>) -> Data<Block> {
        let mut digest = [0u8; BLOCK_DIGEST_SIZE];
        digest.copy_from_slice(block.digest().as_ref());
        digest[BLOCK_DIGEST_SIZE - 1] ^= 1;
        Data::new(Block::clone(block).with_digest(BlockDigest::from(digest)))
    }

    /// Expand a leader block into the twin pair ordered for `peer`, plus the twin to reflect
    /// into the equivocator's own DAG; other messages pass through alone.
    pub(crate) fn rewrite(
        &self,
        message: NetworkMessage,
        peer: usize,
    ) -> (Vec<NetworkMessage>, Option<NetworkMessage>) {
        match message {
            NetworkMessage::Block(block) if self.is_leader_block(&block) => {
                let twin = Self::twin(&block);
                let reflected = NetworkMessage::Block(twin.clone());
                let (first, second) = if peer % 2 == 1 {
                    (twin, block)
                } else {
                    (block, twin)
                };
                (
                    vec![NetworkMessage::Block(first), NetworkMessage::Block(second)],
                    Some(reflected),
                )
            }
            other => (vec![other], None),
        }
    }
}

#[cfg(test)]
mod tests {
    use dag::authority::Authority;
    use dag::block::{Block, BlockReference, data::Data};
    use dag::crypto::BLOCK_DIGEST_SIZE;
    use dag::sync::network::NetworkMessage;

    use super::Equivocator;

    fn block(author: usize, round: u64) -> Data<Block> {
        let includes = (0..3)
            .map(|authority| BlockReference::new_test(authority, round - 1))
            .collect();
        Data::new(Block::new_for_test(
            Authority::from(author),
            round,
            includes,
        ))
    }

    #[test]
    fn twin_differs_only_in_its_digest() {
        let original = block(1, 5);
        let twin = Equivocator::twin(&original);
        assert_eq!(twin.author(), original.author());
        assert_eq!(twin.round(), original.round());
        assert_eq!(twin.includes(), original.includes());
        assert_eq!(twin.transactions(), original.transactions());
        assert_eq!(twin.timestamp_ns(), original.timestamp_ns());
        assert_ne!(twin.digest(), original.digest());

        let mut expected = [0u8; BLOCK_DIGEST_SIZE];
        expected.copy_from_slice(original.digest().as_ref());
        expected[BLOCK_DIGEST_SIZE - 1] ^= 1;
        assert_eq!(twin.digest().as_ref(), &expected);
        // Flipping the same bit twice restores the original digest.
        assert_eq!(Equivocator::twin(&twin).digest(), original.digest());
    }

    #[test]
    fn only_own_leader_blocks_are_rewritten() {
        // Committee of 4, authority 1, two leaders per round: leader slots at
        // rounds r with r % 4 == 1 (offset 0) or (r + 1) % 4 == 1 (offset 1).
        let equivocator = Equivocator::new(1, 4, 2);
        assert!(equivocator.is_leader_block(&block(1, 1)));
        assert!(equivocator.is_leader_block(&block(1, 4)));
        assert!(!equivocator.is_leader_block(&block(1, 2)));
        assert!(!equivocator.is_leader_block(&block(2, 1)));
    }

    #[test]
    fn rewrite_orders_twins_by_peer_parity() {
        let equivocator = Equivocator::new(1, 4, 2);
        let original = block(1, 1);
        let twin_digest = Equivocator::twin(&original).digest();

        let digests = |messages: &[NetworkMessage]| -> Vec<_> {
            messages
                .iter()
                .map(|message| match message {
                    NetworkMessage::Block(block) => block.digest(),
                    other => panic!("unexpected {other:?}"),
                })
                .collect()
        };

        let (to_even, reflected) = equivocator.rewrite(NetworkMessage::Block(original.clone()), 2);
        assert_eq!(digests(&to_even), vec![original.digest(), twin_digest]);
        assert_eq!(digests(&[reflected.unwrap()]), vec![twin_digest]);

        let (to_odd, reflected) = equivocator.rewrite(NetworkMessage::Block(original.clone()), 3);
        assert_eq!(digests(&to_odd), vec![twin_digest, original.digest()]);
        assert_eq!(digests(&[reflected.unwrap()]), vec![twin_digest]);

        // Blocks outside the leader slots and non-block messages pass through alone.
        let (passed, reflected) = equivocator.rewrite(NetworkMessage::Block(block(1, 2)), 3);
        assert_eq!(digests(&passed), vec![block(1, 2).digest()]);
        assert!(reflected.is_none());
        let (passed, reflected) = equivocator.rewrite(
            NetworkMessage::RequestBlocks(vec![*original.reference()]),
            3,
        );
        assert!(matches!(
            passed.as_slice(),
            [NetworkMessage::RequestBlocks(_)]
        ));
        assert!(reflected.is_none());
    }
}
