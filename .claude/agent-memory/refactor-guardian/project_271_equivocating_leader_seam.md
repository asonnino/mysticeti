---
name: 271-equivocating-leader-seam
description: Issue #271 simulator equivocating-leader shim — disabled vs simulated crypto trust boundary, block-store own-block relaxation, and the reflect-into-own-inbound deadlock that try_send prevents
metadata:
  type: project
---

Issue #271 (branch `equivocating-leader`) adds a wire-level Byzantine shim to the simulator plus
two `dag` relaxations that make same-slot twin blocks representable. Related: [[267-commit-path-labels]].

**`CryptoEngine::disabled()` is NOT simulator-only.** `crates/replica/src/replica.rs:93` picks the
disabled engine when `!protocol.require_crypto`. `require_crypto` is `false` for Nemo-Nemo (CFT)
and for Orcaella / Hydrozoan whenever `f == 0` (`crates/consensus/src/protocol.rs`). So anything
keyed off "crypto disabled" is live in real deployments, not just the simulator.

**The trust boundary is `Mode::Disabled` vs `Mode::Simulated`** (`crates/dag/src/crypto.rs`,
`CryptoVerifier::verify`). `Disabled` returns `BlockDigest::synthetic(round, author)`, which keeps
the binding between a received block's digest and its `(author, round)`. Only `Simulated` trusts
`block.digest()`, so the simulator can forge same-slot twins. `CryptoEngine::simulated()` is chosen
first in `replica.rs` from the builder flag `.with_simulated_crypto()`, whose one caller is
`crates/simulator/src/runner.rs`: the clean seam for simulator-only behaviour.

**Why:** an early #271 revision put the `block.digest()` trust on the disabled path, which turns
`Block::verify` step 3 into a tautology under `require_crypto == false`: one peer could inject
unbounded distinct digests per slot (unbounded `BlockStore::index` growth) and forge equivocation
for an honest author. The separate `Simulated` mode is the fix; a refactor that merges the two modes
reintroduces it.

**How to apply:** when a change is justified as "disabled crypto / tests only", check
`require_crypto` first — the justification is wrong for Nemo-Nemo and f=0 configs. Anything that
must stay simulator-only belongs on `Mode::Simulated` / `simulated_crypto`.

**`BlockStore::add_own_index` no longer asserts.** It keeps the first own block per round and
`tracing::error!`s on a second. Readers are safe: `last_own_block` uses a strict `>` so an
equal-round second block never overwrites it; `get_own_blocks` looks up the *kept* digest, which
was really inserted, so its "index corrupted" panic stays unreachable; `add_loaded`/`add_unloaded`
still insert the twin into `index` (the call order is add_own_index *then* index insert), which is
what lets the equivocator resolve causal references to its own twin. WAL replay re-inserts in write
order (`insert_own_block` precedes the twin's `insert_block`), so recovery keeps the same block the
live run kept. In the `equivocating_leader_n20` simulator scenario this ERROR fires once per leader
round — expected noise, not a fault.

**The reflection must never be awaited: `try_send` prevents a single-node deadlock.** The shim
(`crates/simulator/src/network.rs`, `fn outbound`) reflects the twin with `inbound.try_send(twin)`,
where `inbound` is the *same link's* reverse latency channel — the queue the equivocator's own
`connection_task` is responsible for draining. An awaiting `inbound.send` (an earlier #271
revision) deadlocks: `connection_task(p)` blocks in `BlockDisseminator::send_blocks`
(`crates/dag/src/sync/synchronizer.rs`, awaits the shim sender) -> shim task blocks on the
reflection -> latency task blocks -> nobody drains. All channels are bounded at 16, and the peer
need not be involved at all; pre-existing cross-node blocking needs *both* endpoints stuck, this one
does not. Dropping a reflected copy is safe because the twin is reflected on every one of the N-1
links and the block manager deduplicates it. A refactor that turns the `try_send` back into an
await reintroduces the deadlock.
