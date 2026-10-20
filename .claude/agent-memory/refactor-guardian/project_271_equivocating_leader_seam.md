---
name: 271-equivocating-leader-seam
description: Issue #271 simulator equivocating-leader shim — crypto-disabled digest trust reaches production protocols, block-store own-block relaxation, and the reflect-into-own-inbound deadlock shape
metadata:
  type: project
---

Issue #271 (branch `equivocating-leader`) adds a wire-level Byzantine shim to the simulator plus
two `dag` relaxations that make same-slot twin blocks representable. Related: [[267-commit-path-labels]].

**`CryptoEngine::disabled()` is NOT simulator-only.** `crates/replica/src/replica.rs:91` picks the
disabled engine when `crypto_disabled || !protocol.require_crypto`. `require_crypto` is `false` for
Nemo-Nemo (CFT) and for Orcaella / DagHydrangea whenever `f == 0`
(`crates/consensus/src/protocol.rs`). So anything keyed off "crypto disabled" is live in real
deployments, not just the simulator. The `crypto_disabled` *builder* flag, by contrast, has exactly
one caller (`crates/simulator/src/runner.rs`, `.with_crypto_disabled()`) and is the clean seam to
hang simulator-only behaviour on.

**Why:** #271 made `CryptoVerifier::verify` return `block.digest()` instead of
`BlockDigest::synthetic(round, author)` on the disabled path, which turns `Block::verify` step 3
into a tautology. Under `require_crypto == false` that removes the last binding between a received
block's digest and its `(author, round)`: one peer can then inject unbounded distinct digests per
slot (unbounded `BlockStore::index` growth) and forge equivocation for an honest author.

**How to apply:** when a change is justified as "disabled crypto / tests only", check
`require_crypto` first — the justification is wrong for Nemo-Nemo and f=0 configs.

**`BlockStore::add_own_index` no longer asserts.** It keeps the first own block per round and
`tracing::error!`s on a second. Readers are safe: `last_own_block` uses a strict `>` so an
equal-round second block never overwrites it; `get_own_blocks` looks up the *kept* digest, which
was really inserted, so its "index corrupted" panic stays unreachable; `add_loaded`/`add_unloaded`
still insert the twin into `index` (the call order is add_own_index *then* index insert), which is
what lets the equivocator resolve causal references to its own twin. WAL replay re-inserts in write
order (`insert_own_block` precedes the twin's `insert_block`), so recovery keeps the same block the
live run kept. In the `equivocating_leader_n20` simulator scenario this ERROR fires once per leader
round — expected noise, not a fault.

**Reflect-into-own-inbound is a single-node deadlock shape.** The shim
(`crates/simulator/src/network.rs`, `fn outbound`) awaits `inbound.send(twin)` where `inbound` is
the *same link's* reverse latency channel — the queue the equivocator's own `connection_task` is
responsible for draining. Cycle: `connection_task(p)` blocks in `BlockDisseminator::send_blocks`
(`crates/dag/src/sync/synchronizer.rs:82`, awaits the shim sender) -> shim task blocks on
`inbound.send` -> latency task blocks -> nobody drains. All channels are bounded at 16, and the peer
need not be involved at all. Pre-existing cross-node blocking needs *both* endpoints stuck; this one
does not. `try_send` for the reflection is safe because the twin is reflected on every one of the
N-1 links.
