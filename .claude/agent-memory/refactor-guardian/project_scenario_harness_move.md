---
name: scenario-harness-move
description: Consensus scenario tests moved into lib (src/test_scenarios + scenario_tests! per protocol file); how to audit coverage equivalence and the Debug-dedup fragility
metadata:
  type: project
---

Branch test/scenario-harness (audited 2026-10-20) moved the 16 `crates/consensus/tests/*_tests.rs`
into `src/test_scenarios/<name>.rs` and replaced `ConsensusProtocol::all_for_test` /
`all_fast_path_for_test` with per-protocol `scenario_tests!` calls. Audited safe.

**How to audit coverage equivalence:** `cargo test -p consensus -- --list`, then tally by
scenario (expect 33 tests per general scenario, 8 per fast-path one, 428 total at audit time) and by
config. Per-scenario mapping follows from the macro: every config gets the 12 general scenarios,
`fast_path` arm adds 4. Old `n >= 6/9/20` gates == `sizes: [n20]` only because sizes are {4, 20}.

**Dedup:** `run_scenario` dedups leader-count runs on the `leader_count` of the built `Protocol`
(only Cordial Miners collapses). An earlier version keyed on the Debug name
([[protocol-name-consumers]]), which would silently collapse runs if that format changed.

**Side effects to remember:** `consensus/test-utils` feature is now enabled by nothing in the
workspace. Tarpaulin excludes `crates/consensus/src/test_scenarios.rs` and `test_scenarios/**`
explicitly, since `crates/**/tests/**/*` no longer covers the scenario code.
