---
name: StorageKind::Ephemeral depends on dag/test-utils
description: Replica's StorageKind::Ephemeral path calls Storage::new_for_tests, which is gated by the dag crate's test-utils feature. Replica's Cargo.toml does NOT enable the feature; the code compiles only because the simulator crate (always co-built with replica in this workspace) pulls it in transitively via feature unification.
type: project
---

`crates/replica/src/replica.rs` handles `StorageKind::Ephemeral` by calling
`Storage::new_for_tests(authority, metrics.clone(), &committee)`. That constructor is gated
by `#[cfg(any(test, feature = "test-utils"))]` in `crates/dag/src/storage.rs`.

`crates/replica/Cargo.toml`'s `[dependencies]` section does NOT enable `dag/test-utils` —
it's only enabled in `[dev-dependencies]`. The code still compiles in production because
`crates/simulator/Cargo.toml` declares `dag = { workspace = true, features = ["test-utils"] }`
as a normal dependency, and feature unification across the workspace propagates the flag
into `replica`'s compilation of `dag`.

**Why:** this is a compile-time-only coupling. If `replica` is ever used in a workspace
that doesn't also build `simulator` (or some other crate enabling `dag/test-utils`), the
`StorageKind::Ephemeral` match arm will fail to compile.

**How to apply:** if you refactor the replica crate to be self-sufficient or consider
publishing it separately, add `dag = { workspace = true, features = ["test-utils"] }` to
`[dependencies]` or introduce a dedicated `ephemeral-storage` / `in-memory` feature that
conditionally compiles the `Ephemeral` variant. Also worth noting: exposing a test-only
constructor in production code paths is a code-smell — an explicit `Storage::new_ephemeral`
(non-test) API would be cleaner.
