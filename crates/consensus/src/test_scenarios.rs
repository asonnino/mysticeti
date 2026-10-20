// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Protocol-agnostic consensus scenarios; each protocol runs them through [`scenario_tests!`].

use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use dag::{committee::Committee, test_util::committee};

use crate::protocol::ConsensusProtocol;

pub(crate) mod direct_commit;
pub(crate) mod direct_commit_late_call;
pub(crate) mod direct_commit_partial_round;
pub(crate) mod direct_skip;
pub(crate) mod equivocation_certificate_beats_weak_quorum;
pub(crate) mod fast_commit;
pub(crate) mod fast_missed_slow_commit;
pub(crate) mod fast_missed_weak_indirect_commit;
pub(crate) mod idempotence;
pub(crate) mod indirect_commit;
pub(crate) mod indirect_skip;
pub(crate) mod multiple_direct_commit;
pub(crate) mod no_genesis_commit;
pub(crate) mod no_leader;
pub(crate) mod trailing_skip_not_re_yielded;
pub(crate) mod undecided;

/// Runs `scenario` on a committee of size `n` for each leader count `[1, 2, 2n/3+1, n]`.
pub(crate) fn run_scenario(
    scenario: fn(&ConsensusProtocol, &Arc<Committee>),
    n: usize,
    protocol: fn(NonZeroUsize) -> ConsensusProtocol,
) {
    let committee = committee(n);
    // Cordial Miners ignores the requested leader count, so dedup on the count actually used.
    let mut seen = HashSet::new();
    for leader_count in [1, 2, 2 * n / 3 + 1, n] {
        let spec = protocol(NonZeroUsize::new(leader_count).unwrap());
        let used_leader_count = spec
            .to_protocol(&committee)
            .expect("valid protocol")
            .leader_count;
        if seen.insert(used_leader_count) {
            scenario(&spec, &committee);
        }
    }
}

/// Generates the scenario tests for one protocol configuration:
/// `scenario_tests!(name, sizes: [n4, n20], |leader_count| spec)` emits
/// `name::<scenario>::<size>` tests; adding `fast_path` also runs the fast-path scenarios.
macro_rules! scenario_tests {
    ($name:ident, sizes: [$($size:ident),+], $protocol:expr) => {
        $crate::test_scenarios::scenario_tests!(@config $name, [$($size),+], $protocol, [
            direct_commit,
            direct_commit_late_call,
            direct_commit_partial_round,
            direct_skip,
            idempotence,
            indirect_commit,
            indirect_skip,
            multiple_direct_commit,
            no_genesis_commit,
            no_leader,
            trailing_skip_not_re_yielded,
            undecided,
        ]);
    };
    ($name:ident, sizes: [$($size:ident),+], fast_path, $protocol:expr) => {
        $crate::test_scenarios::scenario_tests!(@config $name, [$($size),+], $protocol, [
            direct_commit,
            direct_commit_late_call,
            direct_commit_partial_round,
            direct_skip,
            idempotence,
            indirect_commit,
            indirect_skip,
            multiple_direct_commit,
            no_genesis_commit,
            no_leader,
            trailing_skip_not_re_yielded,
            undecided,
            equivocation_certificate_beats_weak_quorum,
            fast_commit,
            fast_missed_slow_commit,
            fast_missed_weak_indirect_commit,
        ]);
    };
    (@config $name:ident, $sizes:tt, $protocol:expr, [$($scenario:ident),+ $(,)?]) => {
        fn $name(
            leader_count: ::std::num::NonZeroUsize,
        ) -> $crate::protocol::ConsensusProtocol {
            ($protocol)(leader_count)
        }

        mod $name {
            $($crate::test_scenarios::scenario_tests!(@scenario $name, $scenario, $sizes);)+
        }
    };
    (@scenario $name:ident, $scenario:ident, [$($size:ident),+]) => {
        mod $scenario {
            $(
                #[test]
                fn $size() {
                    $crate::test_scenarios::run_scenario(
                        $crate::test_scenarios::$scenario::run,
                        $crate::test_scenarios::scenario_tests!(@size $size),
                        super::super::$name,
                    );
                }
            )+
        }
    };
    (@size n4) => {
        4
    };
    (@size n20) => {
        20
    };
}

pub(crate) use scenario_tests;
