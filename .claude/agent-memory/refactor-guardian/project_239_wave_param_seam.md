---
name: 239-wave-param-seam
description: Issue #239 threads a Copy `Wave` argument through BaseCommitter's decision rules; why it is codegen-neutral and which self.wave reads were deliberately left behind
metadata:
  type: project
---

`BaseCommitter::try_direct_decide` / `try_indirect_decide` / `decide_leader_from_anchor`
take a `wave: Wave` parameter (issue #239) and use it for all round arithmetic
(`number`, `voting_round`, `decision_round`, anchor floor `leader_round + wave.length()`).
Groundwork for a Steelhead committer that passes a per-round ephemeral
`Wave::new(wl(r), r % wl(r), merged)`.

**Why:** decouple the geometry used to *decide* a slot from the geometry stored on the
committer, so wave length can vary per round.

**How to apply:**

- Threading a `Copy` `Wave` (2×u64 + bool, 24 B) through these methods is **not** a
  perf cost: all three inline into `Committer::try_commit`, so the argument never
  reaches an ABI boundary. Measured on aarch64 release (`--emit=asm -C codegen-units=1`,
  `try_commit` body): identical 688-byte frame, identical call multiset, `str` 67 = 67,
  `ldr` 225 → 210, instructions 1641 → 1594. The `wave` copy is loaded once and CSE'd
  instead of being reloaded from `&self` around each intervening call. Reuse this asm-diff
  recipe for any future "pass a small Copy struct instead of reading self" refactor here.
  Recipe gotcha: when diffing HEAD-vs-working-tree via two trees sharing one
  `CARGO_TARGET_DIR`, a fresh `git worktree` checkout has NEWER mtimes than the edited
  main tree, so building the worktree first makes cargo judge the main tree "fresh" and
  silently reuse the worktree's asm (md5-identical output = the tell). Materialize the
  working-tree state by copying the changed files into a second scratch worktree (fresh
  mtimes) instead of building the main tree in place.
- **Two `self.wave` reads were deliberately NOT parameterized** and will diverge the day a
  caller passes a wave different from `self.wave`:
  1. `elect_leader` → `self.wave.is_leader_round(round)` — round arithmetic on the
     *stored* wave. A per-round ephemeral wave must keep leader-round detection and
     decision geometry in agreement, or a slot gets elected under one geometry and
     decided under another.
  2. `is_certificate` / `enough_leader_support` / the `certified` closure in
     `decide_leader_from_anchor` read `self.wave.merged_certificates()`, while
     `wave.voting_round()` uses the *passed* wave's flag. Same function, two sources for
     the same bit.
  Also test-only: `Committer`'s round-depth queries (`committer.rs`) use `bc.wave`.
- Related: [[merged-certificates-seam]].
