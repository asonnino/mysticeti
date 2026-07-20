---
name: 208-commit-consumer-seam
description: "Issue #208 Phase 2 opt-in commit-consumer output seam; None-path inertness reasoning + two-sender drain + simulator determinism argument"
type: project
---

Issue #208 Phase 2 (branch `commit-consumer`) adds an opt-in output seam that streams every
`CommittedSubDag` to a bounded `mpsc::Sender` after its WAL write. Core/syncer stay synchronous.
Related seam-audit pattern to [[189-fast-path-seams]].

**Commit-production call chain (every producer must forward):** `Syncer::add_blocks` /
`force_new_block` / `try_new_block` now return `Vec<CommittedSubDag>` (previously dropped).
`Core::handle_committed_subdag` returns its *input* `committed` back (was returning the throwaway
`Vec<CommitData>` built for `write_commits`; only 2 refs, caller discarded it). The ONLY Syncer
commit-producers are: `CoreThread` AddBlocks/ForceNewBlock arms (`core_thread.rs`), `InlineDispatcher`
add_blocks/force_new_block (`dispatcher.rs`), and the `NetworkSyncer::start` startup `force_new_block(0)`
(`net_sync.rs:79`, forwarded at top of `run()`). Core::add_blocks / Core::try_new_block and the
`replica/tests/core.rs` calls are on `Core`, NOT `Syncer` — unchanged, don't confuse.

**None-path inertness argument (why it's byte-equivalent):** With `commit_consumer = None`, the
returned Vec is just *moved* up the stack (never cloned) and dropped in `forward_commits` after one
always-false `let Some(consumer) = ... else { return }`. `handle_committed_subdag` still builds
`commit_data` for the WAL and drops it at fn end (previously it dropped the input `committed` at fn
end and returned commit_data — symmetric, same alloc/drop count). Empty Vecs don't allocate. No
metric touched (`inc_leader_timeout` unchanged condition; utilization_timers unchanged). No WAL
ordering change.

**Two-sender drain (shutdown correctness):** `NetworkSyncer::start` gives `create_dispatcher` a
`commit_consumer.clone()` and moves the original into `run()`. So the channel has TWO Sender handles:
(1) `run()`'s local, dropped when `main_task` completes; (2) the dispatcher's, dropped when
`ThreadedDispatcher::stop` joins the core thread (CoreThread's `run` returns `self.syncer`, dropping
the rest incl. its sender) or `InlineDispatcher::stop` calls `into_inner`. A drain loop
(`while receiver.recv().await`) only terminates after BOTH drop — i.e. after full `shutdown()`.

**Backpressure/deadlock:** `blocking_send` runs only on the dedicated core `std::thread` (owns syncer,
holds no other lock). `InlineDispatcher` awaits `send` while holding the tokio syncer `Mutex`
(intentional, for commit-order + backpressure); consumer never needs the syncer lock, so no deadlock.
A non-draining full consumer blocks the replica by design.

**Simulator determinism:** `dispatcher.rs` swapped `parking_lot::Mutex` -> `tokio::sync::Mutex`
(+ removed the `parking_lot` dep from `simulator/Cargo.toml`; grep-confirmed no other simulator use).
The executor is single-threaded (thread-local `RefCell` scheduler in `event_simulator.rs`), so locks
are never truly contended. On the None path `forward_commits().await` and `lock().await` resolve
Ready on first poll (no Pending, no executor yield), so the event schedule is unchanged. Startup
commits are forwarded at the top of `run()` before `round_timeout_task`/`cleanup_task`/connection tasks
spawn, guaranteeing startup-before-steady-state ordering.
