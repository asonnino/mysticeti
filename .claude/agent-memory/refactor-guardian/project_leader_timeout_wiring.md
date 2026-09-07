---
name: round_timeout wiring
description: "#241 split: RoundTimeouts{leader 1s, quorum 75ms, quorum_rounds} replaces the single Duration; round_timeout alone still overrides BOTH caps (pre-split configs preserved); EveryNth classifies by own-block round, coinciding with ready_new_block's leader_round only in steady state"
metadata:
  type: project
---

History: `leader_timeout_task` once hardcoded 1s; config-cleanup made it
`Option<Duration>` falling back to `Protocol::default_round_timeout` (1s if
leader_wait, else 75ms); issue #241 (steelhead branch, 2026-09) split it.

Current seam:
- `dag::sync::net_sync::RoundTimeouts { leader, quorum, quorum_rounds:
  QuorumTimeoutRounds }`; `for_round(round)`: None→leader, Every→quorum,
  Modal{cell, canary}→quorum iff async (cell period, 0=∞) and not canaried
  (since acadd7a; EveryNth/Dynamic retired — see [[261-canary-seam]]).
- `Protocol::quorum_timeout_rounds()`: Every if `!leader_wait` (mahi_mahi,
  cordial_async, bb_async); None otherwise; debug_asserts steelhead is None
  (Steelhead builds Modal at the two wiring sites: replica.rs, runner.rs).
  `default_round_timeout` is GONE — consts
  `DEFAULT_LEADER_ROUND_TIMEOUT`=1s / `DEFAULT_QUORUM_ROUND_TIMEOUT`=75ms
  in consensus/src/protocol.rs.
- Sole resolution point: `Replica::start` (replica.rs) — simulator and cli
  both go through it. **Back-compat invariant:** `quorum =
  quorum_round_timeout.or(round_timeout).unwrap_or(75ms)` — a lone
  `round_timeout` override MUST keep covering both caps; breaking that
  `.or(...)` silently changes every pre-split config.

**Why:** the equivalence proof for all pre-#241 protocols is
leader==quorum (override case) or a constant arm (None/Every); only
Steelhead-finite-period actually mixes caps.

**How to apply:** when auditing timeout changes, check (a) the `.or(
round_timeout)` chain survives, (b) `for_round`'s EveryNth predicate stays
identical to `SteelheadSchedule::is_async_round` (both
`is_multiple_of`, round 0 async), (c) the round fed to `for_round` is
`last_own_block_ref().round()` — coincides with `ready_new_block`'s
`leader_round = quorum_round - 1` only in steady state; a lagging replica
transiently classifies by a stale round (liveness cap only, EveryNth arm
only). A lone `quorum_round_timeout` on a plain leader-wait protocol is
inert (quorum_rounds=None never reads it). See [[240-steelhead-seam]].
