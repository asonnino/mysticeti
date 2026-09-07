---
name: 237-probe-replay-seam
description: "#237 probe-aware replay (dbd3686) + retention 512 (c6c7f28): canary-1/None inertness proofs, probe_rate invariants, the wasted probe pass at canary=1, and the unenforced coprimality constraint"
metadata:
  type: project
---

Commits dbd3686 (probe-aware replay) and c6c7f28 (RETAIN_BELOW_COMMIT_ROUNDS
100 -> 512) on the steelhead branch. Audited 2026-09-06.

**Reduction invariant (why canary 1 / None are byte-identical):**
- The extrapolation branch in `replay` (replay.rs ~line 205) requires
  `!is_async && !is_canaried(round) && probes.is_some()`. At canary=Some(1),
  `is_canaried` is true for every round (`is_multiple_of(1)`), so the branch
  short-circuits before destructuring `probes` — never taken. At canary=None,
  `is_canaried` is always false so `probe_rate`'s loop `continue`s every
  round, total stays 0, and `(total > 0).then_some(..)` yields None.
- `direct_commit_evidence` is a textual extraction of the inlined
  direct-commit check (merged + carrier arms verbatim); the `is_vote` closure
  in `replay_slot` moved below the helper call but is only used by the
  certified path — no observable change.

**probe_rate invariants:**
- Counts are u128 probe counts <= window span <= interval + 1 <= 513;
  `count * scale(round)` <= 513 * 2^64 * n, far below u128::MAX. Division
  guarded by the `total > 0` gate; `failed = total - succeeded` safe because
  succeeded increments only in iterations that increment total.
- Pure function of (window, params, period): blocks are sorted by
  (author, digest) at collection; equivocating leader representative is
  min-digest, same convention as `replay_slot`; quorum booleans are
  order-independent (StakeAggregator dedup). Empty round / missing leader =
  failed probe (total += 1, no success) — conservative, no panic.
- Extrapolated values are the exact probability mix of `replay_slot`'s two
  outcomes: success = (scale(dec), scale(dec)) (direct commit), failure =
  (scale(vote), top_scaled) (direct skip). Extrapolated commit == top_scaled
  iff succeeded == 0, so lower slots' anchor search (`commits < top_scaled`)
  treats a fully-failed extrapolation as no commit mass — coherent.
- The probe leader is `Authority::new(round % n)`, matching `replay`'s own
  sync-slot round-robin model (line ~231). Canaried rounds carry honest
  evidence whether executed sync or async (sync rounds always hold the
  leader wait; canaried async rounds hold it for the RR cohort, per #261).

**Findings flagged (not regressions, but real):**
- At canary=Some(1) — the serde default — `probe_rate` is fully computed per
  candidate (support_map + evidence per sync slot) and then never read:
  roughly doubles per-candidate sync-slot work in `choose_period`, once per
  interval inside try_commit. A `canary == 1 => skip probe pass` guard would
  be behavior-neutral. At canary=None the pass is a trivial O(span) loop.
- dbd3686's commit message claims an "alignment constraint" unit test
  (canary coprime to max_period); no such test exists — only
  sparse_canary_probes_enable_the_climb (canary 5, max 8, coprime) and
  starved_probes_stay_async. Nothing in protocol.rs validates coprimality;
  a misaligned config (e.g. canary=2, max_period=8) silently falls back to
  whole-window evidence for the aligned candidates (conservative: pins at
  period 1, never unsafe).

**Retention 512 (c6c7f28):**
- RETAIN_BELOW_COMMIT_ROUNDS has exactly two uses: `Core::cleanup`
  (dag/src/core.rs ~326) and the adaptive interval cap (protocol.rs ~926,
  which imports the dag constant — the interval<=retention coupling is now a
  shared constant, superseding the "no shared constant" note in
  [[243-adaptive-period-seam]]).
- Tightness argument is value-independent: replay runs in try_commit before
  last_decided passes the anchor, so window floor = anchor - interval >
  last_decided - RETAIN = unload threshold; `unload_below_round` unloads
  rounds <= threshold only.
- `unload_below_round` iterates the WHOLE index per call regardless of
  threshold (block_store.rs ~110), so the constant change doesn't alter
  cleanup's per-call cost shape — only steady-state memory (documented:
  rounds * committee * block_size).
- Validation floor relaxed 4x -> 2x max_period in the same PR: strictly
  admits more configs; error reason string changed but nothing asserts it.

Related: [[243-adaptive-period-seam]], [[261-canary-seam]], [[240-steelhead-seam]].
