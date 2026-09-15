---
name: 267-commit-path-labels
description: Issue #267 commit-path tags on LeaderStatus — 24-byte layout budget, fast-beats-slow labelling precedence, and the wire-visible commit_type rename
metadata:
  type: project
---

`LeaderStatus::{DirectCommit,IndirectCommit}` carry a 1-byte `Copy` path tag
(`DirectCommitPath::{Fast,Slow}`, `IndirectCommitPath::{Certificate,WeakQuorum}`) so
`committed_leaders_total{commit_type}` labels six paths.

**Why:** the four old labels could not separate the dual-path protocols' fast/slow direct
commits nor the two indirect rungs (#199/#267).

**How to apply:**
- `size_of::<LeaderStatus>()` is 24 bytes and must stay there — `Data<Block>` is one `Arc`
  (8 B) while the skip/undecided variants are `(u64, u64)` with no niche, so the
  discriminant takes its own 8-byte slot and leaves 7 bytes of padding next to the `Arc`.
  Any further payload added to a commit variant beyond that padding grows the committer's
  `VecDeque<LeaderStatus>` buffer. A `const _: () = assert!(… == 24)` guards it (dag crate).
- Labelling precedence is **fast wins when both rules hold**: `try_direct_decide` tests
  `enough_fast_path_support` first. So `slow-commit` means "no fast quorum *at the tick the
  leader was first decided*", not "no fast quorum ever". Simulator tests rely on this
  (fast-dominant config asserts `slow_commits == 0`).
- The label values are wire-visible: `direct-commit` / `indirect-commit` no longer exist
  (now `fast-commit`, `slow-commit`, `indirect-commit-certificate`,
  `indirect-commit-weak`). Dashboards, alerts or recording rules kept outside this repo and
  filtering on the old values silently return no data. In-repo consumers
  (`MetricsSnapshot`, the Grafana asset) are label-agnostic or were updated.

Related: [[committer-cursor-semantics]], [[189-fast-path-seams]].
