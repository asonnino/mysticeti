---
name: merged-certificates-seam
description: Why Protocol::merged_certificates exactly reproduces the old wave_length==2 geometric special case in BaseCommitter, and the one trace-emission asymmetry it introduced
metadata:
  type: project
---

`Protocol::merged_certificates` (stored in `Wave`, read via `Wave::merged_certificates()`)
replaced the old hidden special case in `BaseCommitter::is_certificate`
("if potential_certificate.round() == leader_block.round() + 1, reduce to is_vote").

**Why the two are provably identical:** both call sites of the old `is_certificate`
(`enough_leader_support`, `decide_leader_from_anchor`) iterate blocks at
`wave.decision_round(wave.number(leader_round))`, and `try_direct_decide` /
`try_indirect_decide` are only ever reached through `Committer::try_commit`, which skips
any round where `BaseCommitter::elect_leader` returns `None`. So `leader_round` is always
a genuine leader round, hence `decision_round == leader_round + wave_length - 1` always,
hence the geometric predicate is exactly `wave_length == 2`.

**How to apply:** if a future change ever calls `try_direct_decide` /
`try_indirect_decide` with a non-leader round, that equality breaks and the flag and the
old geometry diverge. Also: `Wave::new` now `debug_assert!`s `wave_length > 2 ||
merged_certificates`, so any new `wave_length <= 2` protocol must set the flag.

**Known asymmetry (accepted):** `is_certificate` emits
`tracing::trace!("... is a vote for ...")` per matching vote. The old wl==2 path returned
from the special case before reaching that trace, so BlueBottle PS / Orcaella / Nemo-Nemo
now emit TRACE events they did not before. Cost is a cached-callsite level check per
`is_vote` hit, dwarfed by `find_support`'s recursive DAG walk. Not a metric.
