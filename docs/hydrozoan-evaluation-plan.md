# Hydrozoan evaluation plan (temporary working document)

Status 2026-09-15. Untracked scratch file: lists every experiment the Hydrozoan paper
evaluation needs, which existing data can be reused, and what must be run. Numbers for
existing data come from the Orcaella paper's parser (`parse_yaml`: steady-state floor over
the throughput plateau, min p50 / min p90, median tps).

## 0. Prerequisites before any AWS run

| # | Item | Status |
|---|---|---|
| P1 | Commit-path labels, `block_latency_s`, remote scraping | done: #267 closed by PR #269 |
| P2 | Crash-order knob in the orchestrator (`crash_order: region-order`) | done (this branch, closes #270) |
| P3 | Plot pipeline in-repo (`scripts/eval/`) | done (this branch) |
| P4 | Equivocating-leader fault mode in the simulator | filed: #271 (another agent) |

Notes.

- P2: `Settings::crash_order` (`faults.rs`). The default `round-robin` keeps the selection
  order, so crashes cycle through regions and hit Tokyo on the 6th crash; `region-order`
  crashes region by region in `regions` order, so the tail survives until the first five
  regions are exhausted. Region-order runs get a `-region-order` suffix in the measurements
  filename.
- P3: `scripts/eval/plot.py` + `scripts/eval/hydrangea/*.txt` + `README.md`, venv at
  `scripts/.venv-eval` (gitignored), output to the gitignored `plots/`. Validated at zero cost
  by re-rendering the 13 Orcaella figures from `results/results-96dee8d`; `parse_yaml` output
  is identical to the paper's copy on three files. The new metrics from PR #269 are not parsed
  yet.
- P4: today the simulator has only topology faults (`FullMesh`, `OneDown`, `Star`,
  `Partition`).

## 1. Testbed facts that drive the arithmetic

- Regions (settings order): us-east-1, us-east-2, eu-central-1, eu-west-2, eu-west-3,
  ap-northeast-1. Nodes are picked round-robin in that order (`orchestrator.rs:137`), so region
  sizes at n = 50 are 9, 9, 8, 8, 8, 8: **Tokyo tail = 8, nearby = 42.** At n = 10: 2, 2, 2, 2, 1,
  1 (Tokyo 1). At n = 9: Tokyo 1. At n = 7: Tokyo 1. Confirm from the instance list at deploy
  time; the Orcaella data guide said "about 9".
- A quorum Q with x nearby crashes dodges the tail iff `42 - x >= Q` (n = 50, nearby-first order, P2).
- `--loads` is system-wide tx/s split across nodes; 512-byte transactions; load generator
  collocated; `initial_delay` 30 s; scrape every 15 s; `benchmark_duration` 300 s for the n = 50
  runs (Orcaella used 120 s for small committees, 300 s is safer).
- Instance type m5d.8xlarge, one validator per VM, plus one monitoring instance in us-east-1.
- Leader timeout (`round_timeout`) 1 s, 2 leaders per round unless stated.

## 2. Configurations

All at n = 50 unless stated. Hydrozoan `k` is the leftover `n - 3f - 2c - 1` (tight bound, cap
lifted). Thresholds per `protocol.rs`: `q = n-f-c`, `p = (c+k)/2`, `q_fast = n-p`, `q_cert =
(n+f+2)/2`, `q_slow = 2f+c+1`, `q_weak = f+p+1`. Mysticeti: `q = 2n/3+1 = 34`, f = 16.

| Name                       | Protocol      | (f, c, k)   | p   | q   | q_fast      | q_cert | q_slow | q_weak        | fast slack / slow slack | fast dodges tail fault-free?        |
| -------------------------- | ------------- | ----------- | --- | --- | ----------- | ------ | ------ | ------------- | ----------------------- | ----------------------------------- |
| Mysticeti                  | mysticeti     | f = 16      | -   | 34  | -           | 34     | 34     | -             | - / 16                  | slow path: yes (8 spare)            |
| Byz-only                   | dag-hydrangea | (11, 0, 16) | 8   | 39  | 42          | 31     | 23     | 20            | 8 / 11                  | yes, **0 spare** (draft assumed no) |
| Byz-heavy = Orcaella end   | dag-hydrangea | (8, 3, 19)  | 11  | 39  | 39          | 30     | 20     | 20            | 11 / 11                 | yes, 3 spare                        |
| Balanced = Orcaella end    | dag-hydrangea | (6, 6, 19)  | 12  | 38  | 38          | 29     | 19     | 19            | 12 / 12                 | yes, 4 spare                        |
| Crash-heavy = Orcaella end | dag-hydrangea | (2, 13, 17) | 15  | 35  | 35          | 27     | 18     | 18            | 15 / 15                 | yes, 7 spare                        |
| Graded                     | dag-hydrangea | (6, 8, 15)  | 11  | 36  | 39          | 29     | 21     | 18            | 11 / 14                 | yes, 3 spare                        |
| Orcaella (8,3)             | orcaella      | (8, 3)      | -   | 39  | 39 (direct) | -      | -      | 20 (indirect) | 11 / 11                 | yes, 3 spare                        |
| Orcaella (6,6)             | orcaella      | (6, 6)      | -   | 38  | 38          | -      | -      | 19            | 12 / 12                 | yes, 4 spare                        |

Notes.

- With Tokyo = 8 the Byzantine-only config fits its fast quorum in the nearby 42 with zero spare,
  so fault-free it should sit on the 2-round curve, not on Mysticeti's. One nearby crash or one
  slow nearby node pushes it to the tail. The draft's "p = 8 lands on Mysticeti" prediction
  assumed a 9-node tail. Still the first risk to retire.
- "Graded" (6, 8, 15) has the same fast quorum (39) as Orcaella (8, 3) at the same n, but a slow
  path with 3 more crashes of liveness. That isolates "a slow path exists" as the only difference,
  which is the cleanest C2 comparison.
- The k sweep uses (6, 6) at n = 50: k in {0, 6, 8, 10, 12, 19} gives p = 3, 6, 7, 8, 9, 12 and
  q_fast = 47, 44, 43, 42, 41, 38. The tail step is between k = 8 (q_fast 43, must reach Tokyo)
  and k = 10 (q_fast 42, fits exactly).
- Small committee: Hydrozoan (1, 1, 4) at n = 10 has p = 2, q = q_fast = 8; it is the Orcaella (1,
  1) end at n = 10. Alternatives (2, 0, 3) has q = 8 too; (1, 0, k) needs q = 9 and stalls at 2
  crashes. Avoid f = 0 configs (they disable signatures, `require_crypto: f != 0`).

## 3. Existing data: what is reusable

Three Orcaella campaigns exist in `~/GitHub/hybrid-fault-tolerance/data` (`results-96dee8d/{eu-us,global,eu}` is also tracked in this repo under `results/`).

| Campaign | Build | Verdict |
|---|---|---|
| `results-96dee8d/eu-us` | `mysticeti@orchestrator-refinements`, pure consensus | reuse for the motivating table and as endpoint cross-check; never plot next to new runs |
| `results-2a2bc14/eu-us` | `blockchain-validator@2a2bc14`, checkpoint engine | not needed |
| `results-96dee8d/global` | as eu-us, other region set | not needed (Orcaella appendix) |
| `results-96dee8d/eu` | as eu-us, crash-only n = 3 intra-EU | not needed |
| `hydrangea/bench-*.txt` | Hydrangea codebase, end-to-end "to first commit" median | reuse: n = 10 fault-free as a curve; n = 50 and 2-crash runs as table rows only |

Contents of `results-96dee8d/eu-us`: Mysticeti n = 49 x 3 loads; Orcaella (10,0) n = 51,
(8,3) n = 50, (6,6) n = 49, (2,13) n = 50 x 3 loads; small committees with 2 crashes:
Mysticeti n = 10 and n = 7, Orcaella (1,1) n = 9 and (2,0) n = 11 at 10k and 50k/60k.
`results-2a2bc14` holds Mysticeti n = 50 and Orcaella x 4 configs at n = 51, 3 loads; the same
configs measure 339 / 257 ms there vs 367 / 282 ms in 96dee8d at 10k.

Hydrangea numbers (median end-to-end, tps reached):

| Run                                     | Median latency              | tps                          |
| --------------------------------------- | --------------------------- | ---------------------------- |
| n = 10, 0 crashes, 10k                  | 333 ms                      | 9,976                        |
| n = 10, 0 crashes, 50k                  | 328 ms                      | 49,841                       |
| n = 10, 0 crashes, 70k                  | 315 ms                      | 69,821                       |
| n = 10, 0 crashes, 100k                 | 2,623 ms                    | 98,143                       |
| n = 10, 2 crashes, 10k                  | 27,053 ms                   | 7,017                        |
| n = 10, 2 crashes, 50k                  | 75,687 ms                   | 7,393                        |
| n = 50, 0 crashes, 1k / 5k / 10k / 100k | 66.5 / 66.6 / 67.0 / 67.2 s | 267 / 1,330 / 2,595 / 26,378 |

**One build per figure.** The two Orcaella campaigns come from different codebases
(`asonnino/mysticeti@orchestrator-refinements` vs `asonnino/blockchain-validator@2a2bc14`, the
checkpoint/execution build) and differ by 30 to 80 ms on identical configurations; within one
codebase such shifts are AWS WAN placement jitter. Either way the shift is the size of the effect we
measure, so every curve on one figure comes from one commit of this repo and one deployment. Old
data is for tables, cross-checks and sanity, not for mixed plots.

Old eu-us numbers for cross-checks (p50 at 10k / 50k / 100k, ms): Mysticeti n = 49: 367 / 371 / 382.
Orcaella (10,0): 281 / 303 / 329; (8,3): 293 / 316 / 341; (6,6): 282 / 304 / 330; (2,13): 292 / 311
/ 335. Small, 2 crashes, 10k: Mysticeti n = 10: 327; Mysticeti n = 7: 492; Orcaella (1,1) n = 9:
378; Orcaella (2,0) n = 11: 379.

## 4. Experiments

Every run: 6 regions, 512 B, 2 leaders, 300 s, loads as stated. "New" = must be run from one commit of this repo after P1 (and P2 where crashes are involved).

### E1. Healthy network, n = 50 (claim C1, endpoint match)

Figure: latency vs throughput, loads {10k, 50k, 100k}. Curves: Mysticeti; Hydrozoan Byz-only, Byz-heavy, Balanced, Crash-heavy; Orcaella (6,6). Hydrangea in a table row (n = 50 collapses).

| Runs               | Count  | Reuse                                                                                |
| ------------------ | ------ | ------------------------------------------------------------------------------------ |
| 7 curves x 3 loads | 21 new | old eu-us data as cross-check only; Hydrangea table from existing data |

Blue Bottle runs as `blue-bottle-ps` at n = 50 (commit/skip quorum 41, indirect 21); at n = 51 it
equals Hydrozoan (10, 0, 20) exactly.

Expected: all Hydrozoan configs on the 2-round curve (Tokyo = 8, all fast quorums fit), Balanced overlapping Orcaella (6,6) within noise, Mysticeti about 80 ms above. Throughput identical.

### E2. The k knob (claim C3, the spectrum)

Figure: p50/p90 latency vs k at (6, 6), n = 50, 10k tx/s, with Mysticeti and Orcaella (6,6) as horizontal references. k in {0, 6, 8, 10, 12, 19}.

| Runs                | Count | Reuse                                            |
| ------------------- | ----- | ------------------------------------------------ |
| k = 0, 6, 8, 10, 12 | 5 new | k = 19, Mysticeti, Orcaella (6,6) at 10k from E1 |

Expected: k <= 8 on Mysticeti's level (fast quorum must reach Tokyo, so the 3-round nearby slow path
fires first or ties), step down at k = 10, flat to the Orcaella endpoint. Also directly checks that
q_fast > q configurations are never slower than Mysticeti (risk from the draft).

### E3. Crash sweep, n = 50 (claim C2, the money figure)

Figure: p50 latency (and fast-commit share from P1 labels) vs number of crashed validators, 10k tx/s, nearby-first crash order (P2). Curves: Mysticeti; Orcaella (8,3); Hydrozoan Graded (6, 8, 15).

Predicted plateaus (nearby alive = 42 - x):

| x crashes | Mysticeti (q 34)  | Orcaella (8,3) (q 39) | Hydrozoan (6,8,15) (q_fast 39, q 36)                                                     |
| --------- | ----------------- | --------------------- | ---------------------------------------------------------------------------------------- |
| 0..3      | 3 rounds, no tail | 2 rounds, no tail     | 2 rounds, no tail                                                                        |
| 4..6      | 3, no tail        | 2, with tail          | min(2 with tail, 3 no tail)                                                              |
| 7..8      | 3, no tail        | 2, with tail          | 2, with tail                                                                             |
| 9..11     | 3, with tail      | 2, with tail          | 2, with tail                                                                             |
| 12..14    | 3, with tail      | **stalled**           | 3, with tail (slow path only; direct skip dead, crashed-leader slots resolve indirectly) |
| 15..16    | 3, with tail      | stalled               | stalled                                                                                  |
| 17+       | stalled           | stalled               | stalled                                                                                  |

| Runs                                                      | Count  |
| --------------------------------------------------------- | ------ |
| Hydrozoan (6,8,15): x = 0, 2, 4, 6, 8, 10, 12, 13, 14, 16 | 10 new |
| Mysticeti: x = 2, 4, 6, 8, 10, 12, 14, 16 (x = 0 from E1) | 8 new  |
| Orcaella (8,3): x = 0, 2, 4, 8, 11, 12                    | 6 new  |

Also measured here for free: the accepted crashed-leader penalty past p (x = 12..14 for Hydrozoan) via `indirect-skip` counts and block latency.

### E4. Crash and recovery over time (claim C2, no stall)

The orchestrator already implements this: `FaultsType::CrashRecovery { max_faults, interval }` in
`crates/orchestrator/src/faults.rs` kills `max_faults/3` nodes per interval until `max_faults` are
down, then reboots them all and repeats (unit-tested). No simulator needed. Optional: drop if the
budget is tight, E3 already shows the plateaus.

Figure: throughput, p50 latency and fast-commit share vs time. `faults: !CrashRecovery { max_faults:
12, interval: 60s }` kills 4, 8, 12 then recovers all, repeating; 600 s, 10k tx/s. Curves: Hydrozoan
(6,8,15) and Mysticeti.

| Runs            | Count              |
| --------------- | ------------------ |
| 2 protocols x 1 | 2 new (600 s each) |

### E5. Quorum location, small committees (claim C3)

Figure: bars at 10k tx/s, 1 crash and 2 crashes. Bars: Mysticeti n = 10 (q 7), Mysticeti n = 7 (q 5), Orcaella (1,1) n = 9 (q 7), Hydrozoan (1,1,4) n = 10 (q = q_fast = 8).

Predicted (Tokyo = 1 node, nearby-first): with 1 crash Hydrozoan n = 10 dodges Tokyo with 2 rounds
(best of all), Orcaella n = 9 dodges with 2 rounds, Mysticeti n = 10 dodges with 3 rounds. With 2
crashes Hydrozoan n = 10 and Orcaella n = 9 have zero slack and pay 2 rounds with Tokyo (about 378
ms), Mysticeti n = 10 keeps 1 spare and pays 3 rounds without Tokyo (about 327 ms), Mysticeti n = 7
pays 3 rounds with Tokyo (about 492 ms). The draft's expectation that Hydrozoan at n = 10 with 2
crashes lands on Mysticeti n = 10's 327 ms is wrong: its DAG quorum is 8 of 8 alive. Present the
flip honestly; it is the quorum-location claim itself.

| Runs                       | Count | Reuse                                                              |
| -------------------------- | ----- | ------------------------------------------------------------------ |
| 4 configs x {1, 2} crashes | 8 new | old 2-crash numbers (327 / 378 / 492) stay in the motivating table |

### E6. Scalability, n = 10 healthy (claim C4)

Figure: latency vs throughput at n = 10, loads {10k, 50k, 100k}: Mysticeti n = 10, Hydrozoan (1,1,4) n = 10, Orcaella (1,1) n = 10, Hydrangea n = 10 (reused curve).

| Runs               | Count | Reuse                               |
| ------------------ | ----- | ----------------------------------- |
| 3 curves x 3 loads | 9 new | Hydrangea n = 10 from existing data |

### E7. Leaders per round (claim C4)

Figure: p50 latency vs leaders {1, 2, 4} at n = 50, 10k tx/s: Hydrozoan Balanced and Mysticeti.

| Runs                 | Count | Reuse         |
| -------------------- | ----- | ------------- |
| 2 protocols x {1, 4} | 4 new | l = 2 from E1 |

### E8. Byzantine leader (simulator)

Blocked on P4. If the simulator gains an equivocating-leader mode: n = 20, (3, 4, 2) and
Balanced-like config, one leader equivocates every wave; assert the slot is skipped or
slow-committed, neighbouring slots' latency unchanged, rung-2 tie-break exercised. Otherwise drop
the bullet from the evaluation and cite the consensus integration tests in the implementation
section.

### E9. Simulator pre-checks (free, before AWS)

- Crash sweep past p at n = 50 in the simulator (`OneDown`/`Partition` topologies) to size the crashed-leader penalty and confirm the direct skip never fires past p (needs P1 labels).
- Byz-only (11, 0, 16) with one slow nearby node to confirm the "zero spare" prediction.

## 5. Run budget

| Experiment | New runs | Wall time (5.5 min each, 600 s for E4)                               |
| ---------- | -------- | -------------------------------------------------------------------- |
| E1 | 21 | 1.9 h |
| E2         | 5        | 0.5 h                                                                |
| E3         | 24       | 2.2 h                                                                |
| E4         | 2        | 0.4 h                                                                |
| E5         | 8        | 0.7 h                                                                |
| E6         | 9        | 0.8 h                                                                |
| E7         | 4        | 0.4 h                                                                |
| Total | 73 | about 6.9 h of benchmark time, plus deploy, configuration and reruns (71 without the optional E4) |

At 51 m5d.8xlarge on-demand the n = 50 testbed costs on the order of $90 per hour; plan for roughly
twice the benchmark time to cover reruns. E5 and E6 can run on a 10-node testbed after the large one
is destroyed.

## 6. Decisions taken 2026-09-15

1. P2 filed as #270 (crash order) and implemented on this branch; P4 filed as #271 (equivocating leader) for another agent.
2. Blue Bottle added to E1 as a named baseline (arXiv 2511.15361); Orcaella cited as a preprint.
3. Balanced (6, 6, 19) is "the" Hydrozoan curve; Graded (6, 8, 15) is the crash-sweep configuration.
4. No runs for the draft's capped configs; the k sweep covers them. E4 is optional.
5. Paper text updated (uncommitted, `~/GitHub/hydrozoan-paper`): cap lifted to k <= 2f+c in
   `model.tex`; f+c >= p in `intro`, `overview`, `abstract`, `discussion`; `tab:configs` rebuilt
   with tight k, a slow-slack column and the Graded row, tail = 8; new `\para{The two ends of the
   slack}` in `protocol.tex`; new `\para{Two-round-only DAGs}` in `related.tex`; `orcaella` bib
   entry; `evaluation.tex` bullets rewritten to this plan. Build clean, no undefined refs, cspell
   clean.
6. Still open: E8 stays "pending #271"; confirm the 8-node Tokyo tail from the instance list at deploy time.
