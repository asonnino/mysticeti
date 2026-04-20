# Refactor Guardian Memory Index

- [round_timeout wiring](project_leader_timeout_wiring.md) — Option<Duration>; falls back to protocol default_round_timeout (1s Mysticeti, 75ms async)
- [ReplicaParameters unification (resolved)](project_duplicated_replica_params.md) — Historical: duplicate removed by replica/cli split in b465cba
- [StorageKind::Ephemeral + dag/test-utils](project_storage_ephemeral_test_utils.md) — Replica calls Storage::new_for_tests in production paths; compiles only via feature unification from the simulator crate
- [Orchestrator CLI drift](project_orchestrator_stale_cli.md) — Orchestrator assembles replica flags as format! strings; CLI renames/removals fail only at runtime on remote instances
