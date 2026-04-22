# Refactor Guardian Memory Index

- [round_timeout wiring](project_leader_timeout_wiring.md) — Option<Duration>; falls back to protocol default_round_timeout (1s Mysticeti, 75ms async)
- [ReplicaParameters unification (resolved)](project_duplicated_replica_params.md) — Historical: duplicate removed by replica/cli split in b465cba
- [StorageKind::Ephemeral + dag/test-utils](project_storage_ephemeral_test_utils.md) — Replica calls Storage::new_for_tests in production paths; compiles only via feature unification from the simulator crate
- [Orchestrator CLI drift](project_orchestrator_stale_cli.md) — Orchestrator assembles replica flags as format! strings; CLI renames/removals fail only at runtime on remote instances
- [Committer cursor must track skips](project_committer_cursor_semantics.md) — Core.last_decided advances on every yielded leader (commit + skip); advancing only on commits double-counts trailing-skip metrics
- [ready_new_block gate post-last_decided](project_ready_new_block_gate.md) — gate uses last_decided_round, which is >= old commit-only round, so the gate is bounded-less-eager (≤ wave_length - 1 rounds)
