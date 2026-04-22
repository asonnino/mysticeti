---
name: Committer cursor advances on every decision, not just commits
description: DagConsensus::try_commit cursor must track skips as well as commits to avoid metric double-count on trailing skips
type: project
---

Core::try_commit updates its cursor (`last_decided: Option<(RoundNumber, Authority)>`) inside the `.inspect` closure on every leader yielded by the committer — commits AND skips. Pre-PR-#42 the cursor was `last_commit_leader: BlockReference` advanced only from `sequence.last()` after `filter_map(into_decided_block)`, which dropped skips — so a trailing Skip re-yielded on every tick, inflating `committed_leaders_total{commit_type=*-skip}` indefinitely.

**Why:** Committer's iterator filter is `skip_while + skip(1)` keyed on `(round, authority)`. If the caller's cursor doesn't cover a previously-yielded skip, that skip gets re-derived from the (unchanged) DAG and re-emitted to Prometheus on every `try_commit` tick until a commit past that round lands.

**How to apply:** Any refactor touching Core::try_commit, the DagConsensus trait's try_commit signature, or the committer's `skip_while/skip(1)` draining must preserve the invariant "cursor advances on every yielded LeaderStatus (Commit or Skip), updated before the `into_decided_block` filter." If you see a cursor update that reads from the post-filter sequence (only commits), that's the bug pattern.
