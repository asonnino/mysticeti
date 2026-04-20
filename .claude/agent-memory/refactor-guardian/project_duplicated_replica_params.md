---
name: ReplicaParameters unification (resolved)
description: The ReplicaParameters duplication between replica and simulator was removed by the cli/replica crate split in commit b465cba.
type: project
---

Historical note: `ReplicaParameters` used to be duplicated in `crates/replica/src/config.rs`
and `crates/simulator/src/params.rs` because `simulator` could not depend on `replica` without
a cycle (replica owned the CLI which pulled in simulator).

**Resolved:** commit `b465cba feat: split cli and replica crate` split the replica crate into
a pure library (`replica`) and a new binary (`cli`). The simulator now depends on `replica`;
`crates/simulator/src/params.rs` was deleted and `crates/simulator/src/config.rs` imports
`ReplicaParameters` from `replica::config` directly.

**How to apply:** no longer a drift risk — the sole source of truth is
`crates/replica/src/config.rs`. Keep this note only long enough to explain older commits.
