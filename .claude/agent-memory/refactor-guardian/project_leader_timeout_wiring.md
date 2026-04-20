---
name: leader_timeout wiring
description: leader_timeout was hardcoded 1s in leader_timeout_task despite NodeParameters.leader_timeout defaulting to 2s; refactor made the field actually load-bearing.
type: project
---

Prior to the `refactor` branch, `NetworkSyncer::leader_timeout_task` hardcoded
`Duration::from_secs(1)` in `crates/dag/src/sync/net_sync.rs`, while
`NodeParameters::leader_timeout` (serialized into the public config YAML)
defaulted to `2s`. The field was written to disk, visible in config dumps,
but completely ignored at runtime.

**Why:** This mattered for the `refactor` branch audit — the refactor wired
the field end-to-end, which flips the effective default from 1s to 2s for any
deployment that relied on defaults. Benchmarks/orchestrator pin `secs: 1`
explicitly in `crates/orchestrator/assets/node-parameters.yml`, so CI-style
benchmarks are unaffected. But `replica_commit` / `replica_sync` /
`replica_crash_faults` integration tests in `crates/replica/tests/smoke.rs`
now use a 2s timeout (they call `default_leader_timeout() * 5` for their
overall budget, so they self-correct), and `replica/tests/sync.rs` still
passes an explicit `Duration::from_secs(1)`.

**How to apply:** When reviewing any future changes that touch
`leader_timeout_task` or `DagParameters::leader_timeout`, double-check
whether the caller is relying on the (now-live) YAML value vs. an explicit
override. Specifically: bench harness comparisons across the refactor
boundary need the YAML pinned to `1s` to be apples-to-apples.
