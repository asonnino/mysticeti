// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::Protocol;

impl Protocol {
    /// EXPERIMENT, never merge: Mysticeti waiting for `4n/5 + 1` parents; live up to n/5 crashes.
    pub fn mysticeti_wide_parents(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        Self {
            quorum_threshold: 4 * total_stake / 5 + 1,
            ..Self::mysticeti(total_stake, leader_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::committee::Stake;

    use crate::protocol::Protocol;

    #[test]
    fn only_parent_quorum_differs_from_mysticeti() {
        let total_stake: Stake = 46;
        let leader_count = NonZeroUsize::new(1).unwrap();
        let wide = Protocol::mysticeti_wide_parents(total_stake, leader_count);
        let baseline = Protocol::mysticeti(total_stake, leader_count);
        assert_eq!(wide.quorum_threshold, 37);
        assert_eq!(baseline.quorum_threshold, 31);
        assert_eq!(wide.direct_commit_quorum, baseline.direct_commit_quorum);
        assert_eq!(wide.direct_skip_quorum, baseline.direct_skip_quorum);
        assert_eq!(wide.certificate_quorum, baseline.certificate_quorum);
        assert!(wide.fast_path.is_none());
        assert_eq!(wide.anchor_link_size, baseline.anchor_link_size);
        assert_eq!(wide.wave_length, baseline.wave_length);
        assert_eq!(wide.merged_certificates, baseline.merged_certificates);
        assert_eq!(wide.leader_count, baseline.leader_count);
        assert_eq!(wide.pipeline, baseline.pipeline);
        assert_eq!(wide.leader_wait, baseline.leader_wait);
        assert_eq!(wide.require_crypto, baseline.require_crypto);
    }
}
