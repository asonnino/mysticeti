# Refactor Guardian Memory Index

- [round_timeout wiring](project_leader_timeout_wiring.md) — Option<Duration>; falls back to protocol default_round_timeout (1s Mysticeti, 75ms async)
- [ReplicaParameters unification (resolved)](project_duplicated_replica_params.md) — Historical: duplicate removed by replica/cli split in b465cba
- [StorageKind::Ephemeral + dag/test-utils](project_storage_ephemeral_test_utils.md) — Replica calls Storage::new_for_tests in production paths; compiles only via feature unification from the simulator crate
- [Orchestrator CLI drift](project_orchestrator_stale_cli.md) — Orchestrator assembles replica flags as format! strings; CLI renames/removals fail only at runtime on remote instances
- [Committer cursor must track skips](project_committer_cursor_semantics.md) — Core.last_decided advances on every yielded leader (commit + skip); advancing only on commits double-counts trailing-skip metrics
- [ready_new_block gate post-last_decided](project_ready_new_block_gate.md) — gate uses last_decided_round, which is >= old commit-only round, so the gate is bounded-less-eager (≤ wave_length - 1 rounds)
- [#189 fast-path seams](project_189_fast_path_seams.md) — dual-path committer extension; equal-by-construction quorum invariant, inert Option<FastPath> guards, cfg-gating + feature-unification gotchas
- [#208 commit-consumer seam](project_208_commit_consumer_seam.md) — opt-in CommittedSubDag output channel; None-path inertness, two-sender drain, single-threaded simulator determinism
- [#267 commit-path labels](project_267_commit_path_labels.md) — LeaderStatus path tags: 24-byte layout budget, fast-beats-slow labelling, commit_type wire rename
- [Block latency instrumentation](project_block_latency_instrumentation.md) — per-block observe in handle_commit: anchor uniqueness, genesis guard, pre-resolved label children, transaction_time lock scope
- [Commit-path metric pre-resolution](project_commit_path_metric_preresolution.md) — per-authority leader counters panic if committee_size mismatches; zero series from startup make smoke.rs vacuous; benchmark_duration batching rules
- [#271 equivocating-leader seam](project_271_equivocating_leader_seam.md) — "crypto disabled" is NOT simulator-only (require_crypto=false in prod), only Mode::Simulated trusts block.digest(); own-block relaxation reader audit; reflection must stay try_send (deadlock)
- [Simulator run-to-run divergence](project_simulator_nondeterminism.md) — cause is tokio's unbiased `select!` (7 sites); with a `biased;` patch on both sides, output diffing DOES validate a simulator refactor
- [#277 LatencyModel seam](project_277_latency_model_seam.md) — per-link latency; Uniform RNG-stream equivalence, b->a direction in connect, geography validate/expect invariant, serde_yaml `!tag` vs singleton_map rules
- [#281 link queue seam](project_281_link_queue_seam.md) — Sleep-across-select invariants, +task_id wake skew, run_one() semantics, MAX_IN_FLIGHT window, stale ./target gotcha
- [Protocol name consumers](project_protocol_name_consumers.md) — Debug name -> measurement filenames parsed by paper repos; serde tag in campaign configs
- [Protocol module split](project_protocol_module_split.md) — per-protocol impl Protocol files: verified-safe move pattern, test-path rename, mechanical byte-check
- [Scenario harness move](project_scenario_harness_move.md) — scenario tests in lib via scenario_tests!; --list tally audit, Debug-dedup fragility, dead consensus/test-utils
