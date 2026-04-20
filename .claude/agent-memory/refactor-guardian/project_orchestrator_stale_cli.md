---
name: Orchestrator CLI drift vs replica CLI
description: Orchestrator builds command strings as raw format! literals for replica subcommands, so replica CLI flag renames/removals do not fail at compile time — they fail at runtime on the remote instance.
type: project
---

`crates/orchestrator/src/protocol/mysticeti.rs` assembles shell commands that
invoke `./bin/replica run ...` via `format!`. Flag names appear as opaque
strings (`"--committee-path {path}"`, `"--public-config-path {path}"`, etc.),
so `cargo check` / `cargo clippy` will NOT detect it if the replica CLI drops
or renames a flag.

**Why:** This bit the `config-cleanup` branch: the CLI removed
`--committee-path` from `Operation::Run`, but the orchestrator still emits it
in `node_command`. Locally everything compiles and tests pass; the failure
only surfaces when the orchestrator actually runs the binary on a remote
instance, which no test covers.

**How to apply:** When a refactor changes `Operation::Run`, `TestGenesis`, or
`LocalTestbed` flags in `crates/replica/src/cli.rs`, grep the orchestrator
(`crates/orchestrator/src/protocol/mysticeti.rs`) for every affected flag
name and every file path constant that feeds into `format!` calls. Also diff
the set of files emitted by `test_genesis` against the set of files the
orchestrator `scp`-style references — missing files show up the same way.
