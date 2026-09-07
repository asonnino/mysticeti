# Refactor Guardian Memory Index

- [round_timeout wiring](project_leader_timeout_wiring.md) — #241 RoundTimeouts split (leader 1s / quorum 75ms); lone round_timeout must override BOTH caps
- [ReplicaParameters unification (resolved)](project_duplicated_replica_params.md) — Historical: duplicate removed by replica/cli split in b465cba
- [StorageKind::Ephemeral + dag/test-utils](project_storage_ephemeral_test_utils.md) — Replica calls Storage::new_for_tests in production paths; compiles only via feature unification from the simulator crate
- [Orchestrator CLI drift](project_orchestrator_stale_cli.md) — Orchestrator assembles replica flags as format! strings; CLI renames/removals fail only at runtime on remote instances
- [Committer cursor must track skips](project_committer_cursor_semantics.md) — Core.last_decided advances on every yielded leader (commit + skip); advancing only on commits double-counts trailing-skip metrics
- [ready_new_block gate post-last_decided](project_ready_new_block_gate.md) — gate uses last_decided_round, which is >= old commit-only round, so the gate is bounded-less-eager (≤ wave_length - 1 rounds)
- [#189 fast-path seams](project_189_fast_path_seams.md) — dual-path committer extension; equal-by-construction quorum invariant, inert Option<FastPath> guards, cfg-gating + feature-unification gotchas
- [#208 commit-consumer seam](project_208_commit_consumer_seam.md) — opt-in CommittedSubDag output channel; None-path inertness, two-sender drain, single-threaded simulator determinism
- [#239 wave-param seam](project_239_wave_param_seam.md) — Copy Wave threaded through decision rules; codegen-neutral, plus the self.wave reads deliberately left behind
- [merged-certificates seam](project_merged_certificates_seam.md) — why the flag reproduces the old wl==2 special case, and its one trace-emission asymmetry
- [#240 Steelhead seam](project_240_steelhead_seam.md) — per-round wavelength mode; base-path inertness proof, asm parity evidence, quorum-sharing conservativity invariant
- [#243 adaptive-period seam](project_243_adaptive_period_seam.md) — adaptive-off inertness, drain-truncation invariants, interval<=RETAIN_BELOW_COMMIT_ROUNDS coupling
- [#261 canary seam](project_261_canary_seam.md) — Modal timeout variant replaces EveryNth/Dynamic; canary:null equivalence proofs; period=Some(max_period) rewrite is load-bearing for cell init
- [#237 probe-replay seam](project_237_probe_replay_seam.md) — probe-aware replay + retention 512; canary-1/None inertness, wasted probe pass at canary=1, unenforced coprimality
