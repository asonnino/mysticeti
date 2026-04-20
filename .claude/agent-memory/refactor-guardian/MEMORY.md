# Refactor Guardian Memory Index

- [round_timeout wiring](project_leader_timeout_wiring.md) — Option<Duration>; falls back to protocol default_round_timeout (1s Mysticeti, 75ms async)
- [Duplicated ReplicaParameters](project_duplicated_replica_params.md) — Mirror in replica/config.rs + simulator/params.rs to dodge crate cycle; unify after lib+cli split
- [Orchestrator CLI drift](project_orchestrator_stale_cli.md) — Orchestrator assembles replica flags as format! strings; CLI renames/removals fail only at runtime on remote instances
