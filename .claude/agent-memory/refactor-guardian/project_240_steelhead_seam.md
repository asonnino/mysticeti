---
name: 240-steelhead-seam
description: Issue #240 Steelhead static-period committer — why the base protocols stay bit-identical, the asm evidence for perf parity, and the invariants the mode rests on
metadata:
  type: project
---

`Committer` carries `steelhead: Option<SteelheadMode>` (schedule + merged flag + own
`LeaderElector` + leader_count). Steelhead mode: one BaseCommitter per leader offset
(round_offset 0), every round evaluated under an ephemeral
`Wave::new(wl(r), r % wl(r), merged)` — built on the [[239-wave-param-seam]].

**Why base protocols are unaffected:** the normal-mode `try_commit` loop, `get_leaders`
leader_wait path, and every constructor are verbatim; the only additions on the base
path are (a) one `Option` discriminant branch per round in `try_commit`, (b) the
`LeaderIter::Base` wrapper in `get_leaders`, (c) a `steelhead: None` field in every
constructor.

**How to apply (audit evidence, 2026-09, aarch64 release, cgu=1):**
- Leaf binary (`cli`/replica): `ready_new_block` and `LeaderIter::next` have ZERO
  standalone symbols (fully inlined). `NetworkSyncer::start` (encloses the inlined
  syncer loop): 3847→3890 instr, 349→355 cond branches, call profile identical
  (90 distinct / 245 total — no new calls, allocs, or locks). Steelhead work is
  branch-gated (`cmp #2` on the niche discriminant; `udiv` for `%` only on the
  steelhead arm). Base-path dynamic cost: ~1 predictable branch per call + per
  leader element.
- consensus rlib: `find_support` byte-identical (87 instr). BUT inline placement
  flipped: at HEAD `try_direct/indirect_decide` inlined into `try_commit` (1648
  instr); with the two-branch loop they are out-of-line calls (604 + 329 + 851).
  Same source, bounded call overhead vs their DAG-walking bodies — accepted, but a
  future third call site could flip more inlining; re-check if `try_commit` grows.

**Mode invariants (break these and Steelhead is wrong):**
- `BaseCommitter::elect_leader` (and its `self.wave`) is NEVER used in steelhead mode —
  `mode.elect_leader(round, offset)` = `elector.elect_leader(round + offset)`, same
  formula. `is_certificate`'s trace gate still reads `self.wave.merged_certificates()`;
  stays consistent only because merged is protocol-wide (BB pair true/true, MM pair
  false/false).
- Deque order matches normal mode: rounds desc, `enumerate().rev()` offsets high→low,
  push_front → ascending output.
- Degenerate periods are conservative BY CONSTRUCTION because each pair's sync/async
  constructors share all quorums (mysticeti ≡ mahi_mahi: 2n/3+1, anchor 1, merged
  false; bb_ps ≡ bb_async: 4n/5+1 strong, 2n/5+1 weak anchor, merged true) and
  `get_leaders` in steelhead mode ignores `leader_wait` (async round → None ≡ the
  async protocol's None). If a pair's constructors ever diverge in quorums, period-1
  conservativity silently breaks — steelhead_conservativity_tests.rs is the tripwire.
- `is_async_round(0)` is true for any finite period (0 % p == 0); harmless only
  because genesis is filtered from try_commit output.
- Steelhead inherits `leader_wait: true` from the sync constructor. Since #241
  ([[round_timeout wiring]]) finite periods get `EveryNth` timeouts (75ms cap on
  async slots); infinite period stays uniform 1s.
