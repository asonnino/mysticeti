---
name: ready_new_block gate semantics after last_decided refactor
description: ready_new_block uses last_decided (commits + skips) so the gate is strictly >= the old commit-only threshold
type: project
---

`Core::ready_new_block` guard: `quorum_round > last_decided_round.max(1)`.

`last_decided_round` ≥ old `last_commit_leader.round()` because `last_decided` now advances on skips too. Therefore the gate is **less eager** to return true than pre-PR #42 wording, not more eager.

**Why:** Under the old cursor, a trailing skip at round R left the commit cursor at the prior commit's round R' < R, so `quorum_round > R'` was satisfied in the window `(R', R]`. Under the new cursor that window is gone — we wait until `quorum_round > R`.

**How to apply:** The gap is bounded by `wave_length - 1` rounds (a skip at round R is only decided once wave_length further rounds exist). For default Mysticeti (wave_length=3) the delay is ≤ 2 rounds. Not a liveness regression. If future refactors make this gap larger (e.g., longer skip-derivation windows), re-audit this gate.
