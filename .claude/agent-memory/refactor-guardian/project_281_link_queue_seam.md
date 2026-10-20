---
name: 281-link-queue-seam
description: Simulator link delay rework (#281) — Sleep-across-select invariants, executor wake skew, MAX_IN_FLIGHT window, and the stale ./target gotcha that fakes a test failure
metadata:
  type: project
---

`SimulatedNetwork::latency_channel` became a per-link `VecDeque<(deliver_at, msg)>` with a single
head `Sleep` and a `biased` `select!`. Facts worth keeping:

**`Sleep` semantics (crates/simulator/src/executor.rs).** `Sleep` is `Created(d) -> WaitingUntil(abs)`;
the first poll schedules exactly one `Wake(task_id)` event and **ignores the waker**; later polls
before the deadline return `Pending` and schedule nothing. So a `Sleep` kept in a task local and
awaited through a fresh `async { opt.as_mut().unwrap().await }` wrapper each `select!` iteration is
safe — `&mut Sleep` is `Unpin`, the wrapper is dropped, the state survives, one event per head.
The hazard to watch for is the opposite shape: constructing the timer *inside* the `select!` arm
(`C::sleep(delay)`) re-arms it every loop iteration and leaks one scheduled event per iteration.
A task parked on a `WaitingUntil` `Sleep` with **no** waker registered anywhere is still safe,
because the event is already in the heap.

**Wake skew.** `Waker::wake_by_ref` schedules at `+task_id` nanoseconds, so every mpsc wake costs
`task_id` ns of simulated time; `Sleep` events are exact. n = 50 spawns 2450 link tasks, so
high-index links carry a systematic few-µs-per-hop overhead. Pre-existing, not a link-code bug.

**`run_one()`** returns "event queue empty *after* running this event", not "complete". Completion
must be checked before asserting on it, or a main future that finishes on the last event panics.

**Back-pressure.** The link is bandwidth-free only up to `MAX_IN_FLIGHT` (1024) messages per
directed link; past that a slow link *does* hold the sender back (`a_burst_is_bounded` asserts
exactly that). Any doc claiming "never held back by a slow link" is wrong at that boundary.

**Why:** these are the load-bearing invariants of the link; a future refactor that moves the timer
or drops the `biased;` breaks them silently.

**How to apply:** check them whenever `latency_channel`, `Sleep`, or `run_until_complete` is
touched. See [[277-latency-model-seam]] and [[simulator-nondeterminism]].

**Gotcha — stale `./target`.** `/Users/alberto/GitHub/mysticeti/target` holds test binaries built
from a now-deleted scratch worktree, so `env!("CARGO_MANIFEST_DIR")` in
`crates/simulator/tests/simulation.rs` resolves to a missing path and the geography tests fail
with a bogus `NotFound`. Rebuild into a fresh `CARGO_TARGET_DIR` before believing such a failure.
