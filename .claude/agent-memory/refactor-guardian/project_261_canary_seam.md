---
name: 261-canary-seam
description: "#261 canary leader wait (acadd7a): Modal timeout variant replaces EveryNth/Dynamic, equivalence proofs for canary:null and non-Steelhead, and the invariants the wiring rests on"
metadata:
  type: project
---

Commit acadd7a (steelhead branch) adds `canary: Option<NonZeroU64>` (serde default
Some(1), explicit `null` = None) to Steelhead: canaried async rounds keep the leader
wait for the ROUND-ROBIN cohort (never the coin leader). Audited 2026-09-04, SAFE.

**Equivalence proofs (why the old configs are untouched):**
- `Protocol::quorum_timeout_rounds()` reduced to `leader_wait ? None : Every` +
  `debug_assert!(steelhead.is_none())`. Both call sites (replica.rs, runner.rs) match
  on `protocol.steelhead` first and build `Modal` themselves, so the assert is
  unreachable; non-Steelhead mapping is verbatim the old function with steelhead=None.
- `Modal{period cell, canary}` ≡ old `EveryNth(p)` when canary=None & cell=p (never
  written), ≡ old `None` when cell=0, ≡ old `Dynamic(cell)` when canary=None. The
  canary predicate is `round.is_multiple_of(canary)` ANDed out of the async
  classification — same shape in `for_round` (net_sync.rs) and inverted in
  `touches_current_leaders` (conditions.rs).
- Adaptive cell init LOOKS changed (`schedule.period.unwrap_or(0)` vs old
  `adaptive.max_period`) but is identical: `Protocol::steelhead` rewrites
  `period = Some(adaptive.max_period)` when adaptive is set (and rejects an explicit
  period). Anyone touching that constructor breaks the replica/runner cell-init
  equivalence silently.
- `get_leaders` sync arm: `mode.elect_leader(round, off)` → direct
  `mode.leader_elector.elect_leader(round + off)` — identical on sync rounds because
  elect_leader's sync branch IS that call; marginally cheaper (skips the per-offset
  `period_at` reverse scan). On canaried async rounds the RR cohort is deliberate
  (feature), and the commit rule still uses `mode.elect_leader` (coin) at lines
  ~255/~361 — get_leaders' only caller is dag/core.rs `ready_new_block` (pacing).

**Invariants:**
- The period cell now exists for EVERY Steelhead config; static committers hold it but
  never write (apply_pending_period_update guards on pending_anchor/adaptive). The
  simulator adversary builds its OWN cell for static configs (same init, both
  read-only → consistent); shares one only when adaptive.
- Debug tag stability: canary=1 emits no suffix (`-c8` / `-cinf` otherwise) — tested.
  But serde has default WITHOUT skip_serializing_if, so re-serialized configs now emit
  `canary: 1` (cosmetic diff only, round-trips fine).
- Round 0 is canaried under the default (0 multiple of everything): get_leaders(0)
  returns the RR cohort where it used to return None — vacuously satisfied since
  genesis blocks always exist.

Related: [[240-steelhead-seam]], [[243-adaptive-period-seam]], [[round_timeout wiring]].
