---
name: protocol-module-split
description: Splitting Protocol constructors into protocol/<name>.rs impl blocks is a known-safe move; how it was verified
metadata:
  type: project
---

On 2026-10-20 (branch refactor/protocol-module) each `Protocol::<constructor>` moved from
`crates/consensus/src/protocol.rs` into its own private `mod` under `protocol/` as an
`impl Protocol` block, with its tests in a per-file `#[cfg(test)] mod tests`. Verified SAFE.

**Why it is safe:** inherent impls can be split across child modules with no codegen change.
Private items of `protocol` (`ProtocolError::check_wave_length`) are visible to child modules.
Methods stay reachable as `consensus::protocol::Protocol::x` even though the `mod`s are private.

**How to apply:** for similar moves, check byte-identity mechanically (extract each fn chunk
and substring-match against `git show main:<file>`), not by eye. Note that the test paths
change (`protocol::tests::x` -> `protocol::<file>::tests::x`), so grep for filters that use
the old path. Also, main had 10 tests in protocol.rs, not 13 as the task brief claimed;
count them again rather than trusting the brief. See [[protocol-name-consumers]].
