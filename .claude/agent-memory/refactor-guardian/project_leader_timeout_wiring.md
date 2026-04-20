---
name: round_timeout wiring
description: round_timeout is now Option<Duration>; None falls back to the consensus protocol's default_round_timeout (1s for leader_wait, 75ms otherwise). For Mysticeti end-to-end behavior matches the historical hardcoded 1s.
type: project
---

Prior to the `refactor`/`config-cleanup` work, `NetworkSyncer::leader_timeout_task`
hardcoded `Duration::from_secs(1)` regardless of the serialized
`NodeParameters::leader_timeout` field — the field was written to disk but
ignored at runtime.

Current state (post `config-cleanup`):
- `DagParameters::round_timeout: Option<Duration>`, `#[serde(default)]` so
  `None` is the YAML default.
- `Replica::run` / `SimulatedReplica::start` resolve via
  `round_timeout.unwrap_or_else(|| protocol.default_round_timeout())`.
- `Protocol::default_round_timeout` returns `1s` when `leader_wait == true`
  (Mysticeti, Odontoceti, CordialMinersPartiallySynchronous), `75ms` otherwise
  (MahiMahi, CordialMinersAsynchronous).

**Why:** Mysticeti's effective timeout remains 1s — identical to the historical
hardcoded value. But if someone flips `consensus` to `MahiMahi` or
`CordialMinersAsynchronous` without overriding `round_timeout`, the effective
timeout drops to 75ms. Today only Mysticeti is exercised by tests/benchmarks,
so this is latent but real.

**How to apply:** When reviewing refactors that touch round timeouts, check
which protocol resolves the default and whether any caller relies on the old
1s-for-all-protocols behavior. Any bench harness comparing across protocol
variants must pin `round_timeout` explicitly.
