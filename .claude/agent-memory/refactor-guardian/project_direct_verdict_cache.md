---
name: direct-verdict-cache-seam
description: Adaptive-Steelhead scan reuse of try_commit's direct verdicts (reviewed 2026-09-11 on top of f9ce51f) — soundness conditions, hit-rate cliff on starved DAGs, map bounds, metric/memory side effects, audit method
metadata:
  type: project
---

Patch: `decide_rounds` takes the direct step as a closure; `try_commit` records its
DirectCommit/DirectSkip verdicts (rounds >= `agreed_next.0`) in
`SteelheadMode.direct_verdicts: HashMap<(Round, Authority), LeaderStatus>`; the interval scan
(`advance_agreed_output`) reuses one when every slot round is "complete" (view count at the round
== `get_blocks_by_round(round).len()`); `retain` prunes below the agreed cursor per scan.

**Why reuse is exact (and when it is not):**
- `try_direct_decide` reads rounds only via `blocks_by_round(voting|decision)` +
  `blocks_at_authority_round(leader, round)`; everything else follows includes, which stay in
  the causally closed view. Complete voting+decision rounds => view verdict == local verdict.
  Leader-round completeness is NOT needed (a leader block outside the view has no in-view
  voter, so it cannot be supported/certified) — dropping it tripled starved-shape hits, same
  output hash.
- Local verdict monotone as the DAG grows: DirectSkip unconditionally (blame checked first,
  blames never disappear); DirectCommit(B) only while no skip quorum of blames / no second
  certified leader block appears — needs > f equivocating voters (Steelhead pairs share
  equal quorums, fast_path always None). Beyond f, reuse makes the agreed output depend on
  arrival order and suppresses the "More than one certified block" panic. No simulator or
  replica adversary equivocates today.

**Perf shape:** healthy n=50 i=128: ~126/129 slots hit, adaptive try_commit -16% (cgu16) /
-19% (cgu1). Starved (leader orphaned every 3rd round, the targeted-delay shape): 0/129 hits
— orphans make almost every slot's rounds incomplete; completeness pass is pure overhead
(+0.3..0.75% cgu16, noise at cgu1). Map ~= interval x leader_count entries steady state
(grows only if agreed cursor stalls); capacity kept by retain -> no steady-state allocs;
~1-2 SipHash lookups per try_commit vs ~5 ms/call DAG work at n=50.

**Side effects to remember:** the reviewed draft used `get_blocks_by_round(..).len()`, which
materializes blocks (WAL read + `block_store_loaded_blocks` bump for unloaded rounds); landed
commit 031b472 uses the index-only `BlockReader::count_blocks_by_round` instead — keep it that
way. Cached DirectCommit `Data<Block>` Arcs still delay the `global_in_memory_blocks` decrement
for unloaded blocks until pruning (only when agreed lag > RETAIN 512). 031b472 also added
`steelhead_reuse_tests.rs` (block-by-block delivery, <=f equivocators).

**Codegen:** at f9ce51f `decide_rounds` is OUT-of-line (2 callers) at cgu16 and cgu1; the
closure makes one monomorph per caller, both inlined, no closure symbols, try_direct_decide
still a direct out-of-line call. cgu1 also flipped unrelated replay.rs inlining
(support_map 147->773 instr) and outlined apply_period_update (try_commit 3264->1028).

**Audit method:** Mach-O objdump prints `ltmp0` instead of the real symbol when they share an
address — size symbols from `nm -n -m` address deltas instead; use `objdump -d -r` to see real
`bl` targets (relocations). Separate CARGO_TARGET_DIR per (tree, cgu) avoids stale
fingerprints; cgu1 changes crate hashes.

Related: [[243-adaptive-period-seam]], [[240-steelhead-seam]], [[237-probe-replay-seam]].
