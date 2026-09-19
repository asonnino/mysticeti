# Hydrozoan evaluation plan (temporary working document)

Status 2026-09-15. Untracked scratch file: lists every experiment the Hydrozoan paper
evaluation needs, which existing data can be reused, and what must be run. Numbers for
existing data come from the Orcaella paper's parser (`parse_yaml`: steady-state floor over
the throughput plateau, min p50 / min p90, median tps).

## 0. Prerequisites before any AWS run

| #   | Item                                                               | Status                          |
| --- | ------------------------------------------------------------------ | ------------------------------- |
| P1  | Commit-path labels, `block_latency_s`, remote scraping             | done: #267 closed by PR #269    |
| P2  | Crash-order knob in the orchestrator (`crash_order: region-order`) | done (this branch, closes #270) |
| P3  | Plot pipeline in-repo (`scripts/eval/`)                            | done (this branch)              |
| P4  | Equivocating-leader fault mode in the simulator                    | filed: #271 (another agent)     |

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
- P4: `equivocating_leaders` in the simulator config (`crates/simulator/examples/twin.yaml`).
- P5 (found by the simulator campaign, fixed in PR #276): the commit rule ran only with an own
  proposal, so a fast quorum above the clock quorum (k < 2f + c) was seen one round late.
  Campaign build: `0669999` (branch `eval-0669999`); the proper fix is #275.

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
  1. end at n = 10. Alternatives (2, 0, 3) has q = 8 too; (1, 0, k) needs q = 9 and stalls at 2
     crashes. Avoid f = 0 configs (they disable signatures, `require_crypto: f != 0`).

## 3. Existing data: what is reusable

Three Orcaella campaigns exist in `~/GitHub/hybrid-fault-tolerance/data` (`results-96dee8d/{eu-us,global,eu}` is also tracked in this repo under `results/`).

| Campaign                 | Build                                                   | Verdict                                                                                 |
| ------------------------ | ------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `results-96dee8d/eu-us`  | `mysticeti@orchestrator-refinements`, pure consensus    | reuse for the motivating table and as endpoint cross-check; never plot next to new runs |
| `results-2a2bc14/eu-us`  | `blockchain-validator@2a2bc14`, checkpoint engine       | not needed                                                                              |
| `results-96dee8d/global` | as eu-us, other region set                              | not needed (Orcaella appendix)                                                          |
| `results-96dee8d/eu`     | as eu-us, crash-only n = 3 intra-EU                     | not needed                                                                              |
| `hydrangea/bench-*.txt`  | Hydrangea codebase, end-to-end "to first commit" median | reuse: n = 10 fault-free as a curve; n = 50 and 2-crash runs as table rows only         |

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

| Runs               | Count  | Reuse                                                                  |
| ------------------ | ------ | ---------------------------------------------------------------------- |
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

| Experiment | New runs | Wall time (5.5 min each, 600 s for E4)                                                            |
| ---------- | -------- | ------------------------------------------------------------------------------------------------- |
| E1         | 21       | 1.9 h                                                                                             |
| E2         | 5        | 0.5 h                                                                                             |
| E3         | 24       | 2.2 h                                                                                             |
| E4         | 2        | 0.4 h                                                                                             |
| E5         | 8        | 0.7 h                                                                                             |
| E6         | 9        | 0.8 h                                                                                             |
| E7         | 4        | 0.4 h                                                                                             |
| Total      | 73       | about 6.9 h of benchmark time, plus deploy, configuration and reruns (71 without the optional E4) |

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

## 6. Roadmap (status 2026-09-17, tick as runs land)

Decisions taken on the day: loads 100k then 10k, 50k only if budget remains; n = 50 runs 300 s
(first six) then 240 s with a 120 s initial delay, n <= 10 runs 120 s / 30 s; old-campaign lines
are ~30 ms slower at 10k and stay out of the plotted figures (E1 is all this build); no
multi-leader sweep (E7 dropped); E4 dropped; runs go one at a time on the author's "next";
crash points interleaved across protocols per x. Files: `results/results-0669999/`, figures via
`scripts/eval/hydrozoan.py`, simulator campaign `results/sim-0669999/`.

### AWS, n = 50 (E1, E2, E3)

- [x] E1 Mysticeti 10k, 100k (338 / 377 ms)
- [x] E1 Balanced (6, 6, 19) 10k, 100k (255 / 296 ms, fast share 1.0)
- [x] E1 Byz-only (11, 0, 16) 10k, 100k (240 / 281 ms, fast share 0.97 at 100k)
- [x] E1 Orcaella (6, 6) 10k, 100k (254 / 296 ms = Balanced)
- [x] E1 Blue Bottle 10k, 100k (258 / 295 ms)
- [x] E2 k = 0, 8, 10, 12 at (6, 6), 10k (291, 290, 252, 248 ms; k = 19 from E1: 255); E2 complete
- [x] E3 Orcaella (8, 3) x = 10 (424 ms; completes the bar-zoom group)
- [x] E3 Graded (6, 8, 15) x = 0, 2, 4, 6, 8, 10, 11, 12, 14 (239, 245, 299, 310, 401, 399,
  413, 644, 743 ms)
- [x] E3 Mysticeti x = 4, 8, 10, 12, 16 (340, 344, 540, 540, 595 ms); E3 complete
- [x] E3 Orcaella (8, 3) x = 0, 2, 4, 8, 11 (261, 263, 400, 395, 436 ms)
- [x] Cancelled 09-18 (author decision: no further AWS data points; the testbed is in use for
  other experiments). Byzantine-heavy $(8, 3, 19)$, crash-heavy $(2, 13, 17)$ and the Mysticeti
  end $(16, 0, 0)$ stay simulator-only; the red placeholder squares of the resilience-plane figure
  and the two author notes about these runs were removed from the paper. Original plan kept for
  the record:
  Morning of 09-18, in this order, one run per "next", after `remote-testbed start
  --instances 9` and a check that each region has 9 (Tokyo 8 of 50); the first run doubles
  as the jitter probe. Purpose: the resilience-vs-latency figure (each n = 50 configuration
  as a point: tolerated Byzantine, crash budget, fault-free p50) and the E1 gap.
  0. `scripts/eval/ping-matrix.sh` on the dev machine: all-pairs RTT between the active
     instances (`results/ping-matrix-<time>.csv` plus a region-to-region median table),
     copied to the paper's `data/evaluation/`; add ICMP to the `hydrozoan-eval` security
     groups if pings time out. The region medians are the numbers behind "nearby" vs
     "Tokyo" rounds in the text.
  1. `E1-byz-heavy --full`, Hydrozoan (8, 3, 19), 100k + 10k: expect 296 / 255 +- 15 ms, fast
     share 1.0; second same-resilience pair against Orcaella (8, 3) (261 at 10k).
  2. `E1-crash-heavy --full`, Hydrozoan (2, 13, 17), 100k + 10k: same window.
  3. `E1-mysticeti-end --full`, Hydrozoan (16, 0, 0), 100k + 10k: p = 0, fast quorum 50
     (unanimity), clock quorum 34 = Mysticeti's; expect between 290 and 338 ms at 10k and
     never above Mysticeti (338 / 377); the zero-assumption endpoint of the family.
  4. `E3-graded-x00 --loads 100000`: expect 290 to 300 ms, fast share 1.0.
  If us-east-2 capacity fails again, the configurations stay simulator-only and the E1
  bullet is reworded; then `destroy`, revoke the STS token.
- [x] E1 gap closed 09-19 on a deployment matching 09-17 (Balanced probe 257 vs 255): Crash-heavy
  (2, 13, 17) 10k + 100k (255 / 293) and Graded (6, 8, 15) 100k (282) admitted; Byz-heavy =
  Orcaella (8, 3) (261 at 10k, 09-17); the (16, 0, 0) end dropped; 09-18 attempts failed
  (+11 ms offset, then capacity)
- [x] E1 Orcaella (5, 8) 100k + 10k (09-19 ~09:00 UTC, same deployment as the admitted 09-19
  runs): 298 / 254 ms; third Orcaella point for the resilience-vs-latency figure (E7)
- [x] Hydrangea (6, 6, k = 14), n = 45, 10k, 180 s, its own codebase and Fabric tooling on the same
  54 machines (09-19 ~09:55 UTC): end-to-end median 47.6 s, 4,691 tx/s; result file
  `results/hydrangea-2026-09-19/bench-0-45-1-True-10000-512.txt`, copied to the paper's
  `data/evaluation/hydrangea/`
- [x] Testbed stopped 09-19 ~10:00 UTC (54 instances stopped, not destroyed; destroy together with
  the author)
- [ ] Optional if budget remains: 50k points for E1

### AWS, small committees (E5, E6)

- [x] E6 Mysticeti n = 10, 10k and 100k (271 / 322 ms, 11:00 UTC; re-measure next to Hydrozoan)
- [x] E6 Hydrozoan (1, 1, 4) n = 10, 10k and 100k (215 / 268 ms)
- [x] E6 Mysticeti n = 10 rerun at 10k (jitter probe: 271 vs 271 ms, no drift); E6 complete
- [x] E5 1 and 2 crashes: Mysticeti n = 10 (286 / 289), Mysticeti n = 7 (304 / 504),
  Orcaella (1, 1) n = 9 (216 / 402), Hydrozoan (1, 1, 4) n = 10 (223 / 409); E5 complete

### Simulator (E8, E9)
- [x] RTT matrix (measured 09-18 08:19, 08:43, 10:04 and 09-19 07:55, 08:26; CSVs in
  `results/` and the paper's `data/evaluation/`; medians stable within 2 ms across days):
  `scripts/eval/ping-matrix.sh` on the dev machine (written 09-17, review pending). Every
  active instance pings every other one (full mesh of the 54 public IPs, the addresses the
  validators use, so intra-region pairs are included), `ping -n -q -c 20 -i 0.2 -W 2`, eight
  destinations at a time per source, all sources in parallel: about 30 s. Output
  `results/ping-matrix-<UTC stamp>.csv`, one row per directed pair: src_region, src_ip,
  dst_region, dst_ip, sent, received, min_ms, avg_ms, max_ms, mdev_ms; prints the
  region-to-region median matrix and the pairs that lost probes. The simulator uses avg/2 as
  one-way delay and mdev as jitter width; min/max/loss are the sanity check. If a region is
  down, fill its rows from a public inter-region latency table and mark them in the CSV.
- [ ] Calibrated-topology campaign (after the 09-18 ping matrix; needs simulator changes:
  region assignment round-robin as the testbed, per-pair delay = RTT/2 +- jitter,
  nearby-first crash order; sim load 200 tx/s per validator = 10k total). Run groups:
  1. Validation, 2 seeds (~90 runs): the seven fault-free n = 50 configurations
     (targets Mysticeti 338, Balanced 255, Orcaella (6,6) 254, BB 258, Byz-only 240,
     Graded 239, Orcaella (8,3) 261), k sweep (6,6,k) k in {0,8,10,12} (292/290/252/248,
     step at 10), E3 sweeps Graded x in {0..14 as AWS}, Orcaella (8,3) x in {0..11},
     Mysticeti x in {4..16} (steps at 4 and 8, stalls at 15/12/17), E5/E6 small committees
     (Tokyo 1 of n) fault-free and 1-2 crashes. Pass: within ~10% of AWS p50 and the same
     step positions.
  2. Heat map, 1 seed (~450 runs): every tight split 3f + 2c + 1 <= 50 (k = 49 - 3f - 2c,
     ~225 cells incl. (16,0,0) = Mysticeti end), (a) fault-free, (b) x = f + c nearby-first
     crashes (liveness bound). Colour = p50 latency; stalled cells marked.
  3. Equivocation on the calibrated topology, 2 seeds (~32 runs): all six protocols with 1
     and with 2 nearby equivocators (the 09-16 campaign ran 2 only for Graded/Balanced, a
     scope choice, not a constraint: every baseline tolerates 2 Byzantine; the missing
     baselines leave Hydrozoan's 2-equivocator p90 jump without comparison), Graded x = 8 +
     1 equivocator, plus Graded with a Tokyo equivocator. Replaces Table V numbers.
  4. Optional (~90 runs, 1 seed): full crash sweeps x = 0..bound+1 for the six paper
     configurations on the calibrated topology (geo version of Fig 10a).

- [x] Crash sweep past p and liveness bounds, six protocols, two seeds (fixed build)
- [x] Equivocating leaders, all protocols; two equivocators; equivocation plus 8 crashes
- [ ] Pick the rows and figures the paper uses (`plots/sim-0669999/`)

### Wrap-up

- [ ] Destroy the testbed (`remote-testbed destroy`) and revoke the dev-machine STS token
- [ ] Review and commit `scripts/eval/{aws_campaign.py,hydrozoan.py,sim_campaign.py,sim-remote.sh,
  settings-eval.yml}`, `results/results-0669999/`, `results/sim-0669999/`
- [ ] Paper: `evaluation.tex` bullets to prose, figures E1/E2/E3/E5/E6 + simulator tables; drop
  the "more leaders per round" clause of C4; state the x = 7..8 crossing where Mysticeti's
  smaller clock quorum still dodges Tokyo

## 7. Findings (appended as runs land, 2026-09-17)

- Build offset: the old campaign runs ~30 ms slower at 10k than this build on the same regions
  and machine type (Mysticeti 367 vs 338 ms), and ~5 ms slower at 100k (382 vs 377). Same shift
  for every protocol, so old lines are cross-checks only; every plotted curve is this build.
- Commit-trigger artefact (fixed in PR #276, tracked for a proper fix in #275): the commit rule
  ran only with an own proposal, so a fast quorum above the clock quorum (k < 2f + c) was
  observed one round late. In the simulator Graded (6, 8, 15) and Byz-only (11, 0, 16) read 241
  and 250 ms fault-free against 185 for Balanced; with the fix all three read 183 to 186 ms.
- E1 (n = 50, healthy): Balanced (6, 6, 19), Orcaella (6, 6) and Blue Bottle coincide to within
  4 ms at both loads (254 to 258 ms at 10k, 295 to 296 at 100k), the endpoint match of
  \Cref{sec:thresholds}; Byz-only (11, 0, 16) is 15 ms lower still (240 / 281). Mysticeti is
  338 / 377. Throughput plateau 100k for all. Leader block latency: 119 to 143 ms for the
  two-round protocols vs 227 ms for Mysticeti at 10k; queuing adds 110 to 130 ms on top of
  every protocol alike.
- Zero-spare configuration under load: Byz-only's fast share is 1.00 at 10k but 0.97 at 100k,
  so 3 % of slots fall to the slow path when any of the 42 nearby validators straggles.
- Aggregation: the first scrapes of a run still carry the previous run's rates; the plateau
  detection must discard scrapes before the initial delay elapsed (done in `hydrozoan.py`).
- E3 (Graded (6, 8, 15), nearby-first crashes, 10k): x = 0, 2 on the two-round curve (239, 245
  ms); x = 4, 6 in the mixed regime where 3 nearby rounds tie with 2 rounds through Tokyo,
  the two paths split the slots 55/45 and p50 is 299 to 310 ms; x = 8, 10 on "2 rounds with
  tail" at 399 to 401 ms with fast share back to 1.0. Direct-skip share equals x/50 at every
  point: crashed slots are direct skips up to p = 11.
- The fast-commit share is a race diagnostic, not a health metric: it dips at x = 4 to 6 and
  returns to 1.0 at x = 8 because the slow path stops being competitive once every round
  carries the tail. Read block latency (119, 121, 182, 200, 234, 237 ms) for the cost.
- Crossing with Mysticeti at x = 7 to 8: Mysticeti's clock quorum of 34 still fits the 34
  nearby survivors at x = 8 (344 ms, three nearby rounds) while Graded's quorum of 36 needs
  Tokyo on every round (401 ms). Two crashes of slack (f + c = 14 vs 16) move the tail boundary
  by two; the paper must state it. Orcaella (8, 3), quorum 39, pays the tail from x = 3 and sits
  on Graded's point at x = 8 (395 ms).
- Crossing reversed at x = 10: Mysticeti's quorum of 34 no longer fits the 32 nearby survivors,
  every round carries Tokyo and three tail rounds cost 540 ms (block 352 ms), against Graded's
  two tail rounds at 399 ms (block 237). From here on the fast path is worth about 140 ms per
  commit, the size of one Tokyo round trip.
- Past p (Graded x = 12, 38 alive): fast share 0, every commit on the slow path, crashed slots
  resolved by indirect skips (share 0.24). p50 644 ms, p90 928 ms, block 441 / 677 ms, against
  Mysticeti's three tail rounds at 540 ms (x = 10). The indirect-skip penalty on a WAN is
  therefore ~100 ms at p50 and ~250 ms at p90, not the 15 to 20 ms the simulator measured; the
  text must quote the measured number. Liveness and consistency hold.
- Same-x comparison past p (x = 12): Mysticeti 540 / 658 ms (block 355, direct skips 0.24, its
  skip quorum 34 still fits 38 alive), Graded 644 / 928 ms (block 441, indirect skips 0.24). The
  measured crashed-leader penalty is +104 ms at p50 and +270 ms at p90; Mysticeti's own curve is
  flat from x = 10 to 12 (540 ms both).
- Liveness bound (Graded x = 14, 36 alive = q): live and consistent at p50 743 ms, p90 1177 ms,
  block 460 / 732 ms, throughput 7.2k for 36 load generators, indirect skips 0.28. With zero
  spare the clock waits for the slowest live validator every round, hence the extra 100 ms
  over x = 12. Stall at x = 15 is the simulator's job (not spent on AWS).
- Why Mysticeti beats Graded at x = 12 (a different mechanism from x = 8): both clocks pay the
  tail, but past p both of Hydrozoan's direct rules die together. The fast commit needs 39 votes
  and the direct skip needs n - p = 39 blames, and only 38 validators exist, so every commit
  takes Mysticeti's three-round rule and every crashed slot (24 % of slots) waits for a later
  anchor to be skipped indirectly, holding the healthy leaders queued behind it. Mysticeti's
  single quorum of 34 keeps its direct skip alive to its own bound. On a WAN the extra anchor
  wave is a Tokyo round trip, hence +104 ms p50 / +270 ms p90; the simulator's 15 to 20 ms
  reflected its 50 to 100 ms links. Text: "past p Hydrozoan degrades to Mysticeti's slow path
  plus an indirect-skip penalty of ~100 ms at p50 (270 ms at p90) at n = 50, and stays live to
  14 crashes where Orcaella (8, 3) has stalled since 12".
- Mysticeti at its bound (x = 16, 34 alive = q): live at p50 595 ms, p90 1079 ms, block 379 /
  477 ms, direct skips 0.32. Symmetric to Graded at x = 14 (743 / 1177): each protocol slows by
  ~100 ms at p50 and doubles its p90 when its clock quorum loses its last spare. Hydrozoan
  lives to 14, Mysticeti to 16, Orcaella (8, 3) to 11.
- Orcaella (8, 3) at its bound (x = 11, 39 alive = q): live at p50 436 ms, p90 559 ms, block
  251 / 308, direct skips 0.22 (skip quorum 4f + 2c + 1 = 39 still fits). Two tail rounds at zero
  spare; stalls at x = 12 where Graded measured 644 ms and Mysticeti 540 ms. The three bounds
  (Orcaella 11, Hydrozoan 14, Mysticeti 16) are the resilience axis of the spectrum.
- At p (Graded x = 11, 39 alive = fast quorum): fast share 0.97, slow 0.03, direct skips 0.22,
  p50 413 ms, p90 547, block 242. The fast path holds with zero spare (like Byz-only at 100k)
  and Hydrozoan is 23 ms under Orcaella (8, 3) at the same x (436). The past-p cliff is the
  measured step 413 -> 644 ms between x = 11 and 12; without this point the interpolated line
  wrongly showed Orcaella ahead at x = 11.
- Orcaella (8, 3) fault-free: 261 / 361 ms, block 148, on the two-round curve with Graded (239)
  and Balanced (255).
- Slow path vs none at the same fast quorum (x = 4): Orcaella (8, 3), quorum 39, is past its
  tail boundary (x = 3) and pays two Tokyo rounds, 400 / 504 ms (block 233). Graded (6, 8, 15),
  same fast quorum 39 but clock quorum 36 and a slow path, measured 299 / 417 (block 182) at the
  same x: the 45 % of slots the fast path loses to the tail are committed by three nearby
  rounds instead. About 100 ms for x = 4 to 6, the cleanest C2 evidence in the sweep.
- Clarification of the x = 4 to 6 gap (Orcaella 400 vs Graded 299 ms): neither protocol commits
  indirectly before p; both are 100 % direct. Orcaella's clock quorum equals its fast quorum
  (39), so with 38 nearby alive every ROUND waits for Tokyo and its two-round rule costs two
  tail trips. Graded's clock quorum is 36, so rounds stay nearby, and per slot the three-round
  slow direct rule (~300 ms nearby) beats the two-round fast rule (~400 ms via Tokyo) about
  half the time. The slow path buys both the smaller clock quorum and the rule that exploits
  it. Orcaella x = 2: 263 / 359 ms (block 160), still nearby; its line is complete (0, 2, 4, 8,
  11, stall at 12).
- E3 complete (2026-09-17): Mysticeti flat at 338 to 344 ms from x = 0 to 8, 540 at x = 10 to
  12, 595 at its bound (16). Hydrozoan Graded below it for x <= 6 and x = 9 to 11, above it at
  x = 7 to 8 (clock quorum 36 vs 34) and past p (indirect-skip penalty). Orcaella (8, 3) equals
  Hydrozoan fault-free, is 100 ms slower at x = 4 to 6, equal at 8 to 11, and stalls at 12.
- Why Graded beats Mysticeti at x = 4 (299 vs 340 ms) although both clocks are nearby: Mysticeti
  has one rule (three nearby rounds, 340). Graded races two direct rules per slot and takes the
  first: the slow rule (identical to Mysticeti's, 340) or the fast rule (one nearby round plus
  one Tokyo trip for the 39th vote), which wins ~55 % of slots. The mixture gives 299 / block
  182 vs Mysticeti's 234. With equal clocks Hydrozoan is never slower than Mysticeti; it is
  slower only where its clock quorum is larger (x = 7 to 8) or its direct rules are dead (> p).
- E2 k = 10 at (6, 6): 252 / 362 ms, block 142 / 218, fast share 0.92, slow 0.08. The step down
  to the two-round curve is complete at the tail boundary (fast quorum 42 = nearby 42); the 8 %
  slow share is the zero-spare signature (any nearby straggler hands the slot to the slow path).
- E2 k = 8 at (6, 6): 290 / 389 ms, block 175 / 247, fast 0.49 / slow 0.51. Fast quorum 43 must
  reach Tokyo, so it ties with three nearby rounds and the paths split the slots: the mixed
  regime seen at E3 x = 4 to 6, sitting between the two-round curve (255) and Mysticeti (338).
  The k step is therefore soft on the low side (k = 8 is already 48 ms under Mysticeti), not a
  cliff; the "never above Mysticeti" half of C3 holds.
- E2 k = 12 at (6, 6): 248 / 354 ms, block 136 / 205, fast 0.97, slow 0.03. One validator of
  spare inside the nearby 42 (fast quorum 41) cuts the zero-spare slow share of k = 10 (8 %) to
  3 %; latency unchanged. So the k knob has two visible effects: the fast quorum crossing the
  tail (k < 10 vs >= 10) sets the level, and the spare (k - 10) sets how often a straggler
  hands a slot to the slow path.
- E2 complete: k = 0 and 8 both 290 / 391 ms (block 176, fast 0.48 / slow 0.52), k = 10, 12, 19
  at 248 to 255. The sweep is a step at the tail boundary, not a slope: below it the fast
  quorum needs a Tokyo vote and the fast rule wins ~half the slots against the three-round
  nearby slow rule (mixed regime, between Orcaella 254 and Mysticeti 338); at or above it the
  fast quorum fits the 42 nearby validators and every k lands on the two-round curve.
- Time-of-day: WAN jitter grows from ~16:00 UK (US/EU overlap). Runs compared on one figure
  should share the same hour; the E6 pair (Mysticeti n = 10 measured 11:00 UTC) is re-measured
  together with Hydrozoan n = 10 rather than mixed across the day.
- Orcaella (8, 3) x = 10: 424 / 549 ms, block 245, direct skips 0.20. 25 ms above Graded's 399
  at the same x: both pay two tail rounds, but a clock quorum of 39 needs 7 of the 8 Tokyo
  blocks per round where Graded's 36 needs 4. Explainable; the bar zoom's x = 10 group is
  complete.
- E6 Hydrozoan (1, 1, 4) n = 10, 100k (15:10 UTC): 100k sustained, 268 / 403 ms, block 154,
  fast share 1.0, vs Mysticeti n = 10 at 100k 322 / 465 (block 201) from 11:00 UTC: 54 ms
  under, the two-round advantage at n = 10 and full load (C4). The 100k point, the most
  jitter-sensitive, matched its expectation, so the afternoon data is still clean.
- E6 Hydrozoan (1, 1, 4) n = 10, 10k: 215 / 330 ms, block 113, fast share 1.0, vs Mysticeti
  271 / 374 (block 176): 56 ms under at 10k, 54 under at 100k. The fast-path advantage holds
  at n = 10 (C4). Leader block latency 113 ms is two nearby rounds at n = 10.
- Jitter probe (Mysticeti n = 10, 10k, same ten machines): 271 / 369 ms at 15:25 UTC vs
  271 / 374 at 11:00 (block 179 vs 176). No drift on 2026-09-17; every run of the day is
  comparable. Probe file kept separately in `results/results-0669999-probe/`.
- E5 (small committees, Tokyo = 1 validator, 10k): Hydrozoan (1, 1, 4) n = 10 with 1 crash:
  223 / 335 ms, block 142, fast 1.0, direct skips 0.10; the fast quorum of 8 fits the 8
  nearby survivors, so it dodges Tokyo (8 ms above fault-free).
- E5 Hydrozoan (1, 1, 4) n = 10 with 2 crashes: 409 / 560 ms, block 246, fast 1.0, direct
  skips 0.20. Zero slack: the fast quorum of 8 equals the 8 alive including Tokyo, so every
  commit takes two Tokyo rounds (+186 ms over 1 crash). The draft's expectation of landing on
  Mysticeti n = 10's ~327 was wrong for the reason the plan gave: q = 8 of 8 alive.
- E5 Orcaella (1, 1) n = 9 with 1 crash: 216 / 334 ms, block 135, direct skips 0.11. Quorum 7
  equals the 7 nearby survivors: dodges Tokyo, on par with Hydrozoan n = 10 (223).
- E5 Orcaella (1, 1) n = 9 with 2 crashes: 402 / 531 ms, block 246, direct skips 0.22: zero
  slack, two Tokyo rounds, on par with Hydrozoan n = 10 (409). Old campaign had 378.
- E5 Mysticeti n = 10 with 1 crash: 286 / 401 ms, block 193, direct skips 0.10. Three nearby
  rounds (quorum 7 fits the 8 survivors): 63 to 70 ms behind Hydrozoan n = 10 (223) and
  Orcaella n = 9 (216) at one crash.
- E5 flip measured: Mysticeti n = 10 with 2 crashes 289 / 409 ms (block 206; quorum 7 fits the
  7 nearby survivors, three nearby rounds) vs Orcaella n = 9 402 and Hydrozoan n = 10 409 (two
  Tokyo rounds, zero slack). With 1 crash the order is reversed (286 vs 216 / 223). Latency
  follows whether the firing quorum reaches the tail, not the round count (C3). Old campaign
  had 327 for this Mysticeti point.
- E5 Mysticeti n = 7 with 1 crash: 304 / 426 ms, block 222, direct skips 0.14. Quorum 5 fits
  the 5 nearby survivors, so three nearby rounds like n = 10 (286): the small committee only
  pays the tail from the second crash.
- E5 complete. Mysticeti n = 7 with 2 crashes: 504 / 659 ms, block 333 (three Tokyo rounds;
  old table 492). New same-build motivating table at 10k, 2 crashes: Mysticeti n = 10 289,
  Orcaella (1, 1) n = 9 402, Hydrozoan (1, 1, 4) n = 10 409, Mysticeti n = 7 504; at 1 crash:
  286, 216, 223, 304. Replaces the old 327 / 378 / 492 in \Cref{tab:quorum-location}.
- 2026-09-17 16:10 UTC: attempt to close the E1 gap (Byz-heavy (8, 3, 19), Crash-heavy
  (2, 13, 17), Graded 100k) blocked by `InsufficientInstanceCapacity` for m5d.8xlarge in
  us-east-2 (2 of 9 came back; 47 active). Not run: with 47 instances the round-robin gives
  Tokyo 9 validators and breaks the quorum arithmetic shared by every other E1 point. Testbed
  stopped for the night (disks kept); retry `start --instances 9` in the morning; if capacity
  or comparability fail, the two configurations stay simulator-only and the E1 bullet is
  reworded.

- RTT matrix 2026-09-18 08:19 UTC (54 instances, 2862 directed pairs, 20 probes each, 2 pairs
  lost one probe; `results/ping-matrix-2026-09-18T0819.csv`, copied to the paper's
  `data/evaluation/`). Median RTT in ms between regions:

  | | ap-northeast-1 | eu-central-1 | eu-west-2 | eu-west-3 | us-east-1 | us-east-2 |
  | --- | --- | --- | --- | --- | --- | --- |
  | ap-northeast-1 | 0.3 | 238.3 | 212.2 | 219.2 | 146.4 | 134.8 |
  | eu-central-1 | 238.3 | 0.4 | 12.0 | 8.2 | 90.6 | 95.6 |
  | eu-west-2 | 212.2 | 12.0 | 0.2 | 7.4 | 75.3 | 86.1 |
  | eu-west-3 | 219.2 | 8.2 | 7.4 | 0.1 | 82.2 | 90.7 |
  | us-east-1 | 146.4 | 90.6 | 75.3 | 82.2 | 0.2 | 11.4 |
  | us-east-2 | 134.8 | 95.6 | 86.1 | 90.7 | 11.4 | 0.6 |

  Reading: the 42 nearby validators (US + EU) are within 96 ms RTT of each other, Tokyo is
  135 to 238 ms from all of them. A nearby round (one-way, worst of the quorum) is ~45 to
  50 ms; a round that must include Tokyo adds ~70 to 120 ms one way, i.e. the 100 to 140 ms
  steps of E3 are one Tokyo round trip. Parser note: Linux ping appends ", pipe N" when the RTT
  exceeds the probe interval; the first matrix of the morning parsed those rows wrongly (fixed in
  `ping-matrix.sh`, matrix re-measured).

- Simulator probes 2026-09-18 (main @ 123e309, `latency.geographic` from #277, four local runs
  before any campaign, `results/sim-probe/`): Graded (6, 8, 15) fault-free n = 50 is sane (leader
  p50 187 ms pooled over all replicas, fast share 0.99, one skip); Graded x = 8 confirms the
  crashed set is us-east-1 (indices 0, 6, ..., 42) and that replica A's metrics are useless there
  (A is crashed: `sim_campaign.py parse` now pools the latency histograms of every alive replica
  and takes the commit counters from the first alive one). Mysticeti n = 10 fault-free is broken:
  59 direct skips of leader I (index 8, Frankfurt), 60 leader timeouts per replica, leader p50
  411 ms / p90 1149 ms against 271 ms and no skips on AWS. Variants isolate the cause to the mix
  of fast and slow links (no Tokyo, all-100 ms, uniform 50-100: all clean): the delay channel
  sleeps per message sequentially, so a link carries at most one message per latency and the
  Tokyo links (8-15 msg/s capacity) saturate under nearby rounds (~22 blocks/s). A per-message
  delay in a throwaway worktree removes every skip and timeout (n = 10: e2e p50 259-269 ms nearby,
  313 ms Tokyo). Filed as asonnino/mysticeti#281; the campaign waits for it. Issue #278 (same-seed
  nondeterminism, ~1%) is not blocking: seeds are treated as independent samples.

- Fixed-channel probes 2026-09-18 (same four configs, per-message delay from the #281
  experiment, e2e p50 pooled over all alive replicas vs AWS 10k e2e p50): Graded (6, 8, 15)
  x = 0: 247 vs 239 ms; Graded x = 8: 429 vs 401 (the Tokyo step, +182 vs +162); Mysticeti
  n = 50: 318 vs 338; Mysticeti n = 10: 267 vs 271. All within 7% with `extra_ms` 0-1 and no
  processing model, fast shares 1.0 / 1.0 / 0 / 0, zero skips except the 320 crashed-leader
  slots of x = 8. Leader block latency in the simulator (125 / 242 / 209 / 172 ms) sits below
  AWS's block latency as expected (no execution or disk); compare e2e to e2e. Wall time per run
  on the laptop: Graded 4 min, Mysticeti n = 50 18 min (task per message in the experiment
  binary; the real fix may be cheaper) -> the heat map (450 runs) needs a 64-vCPU box for ~1 h,
  Mysticeti-heavy groups longer.

- Batch 1 on main @ 649ec4f (#282 landed, per-message link delays), 2026-09-18 ~14:30 UTC,
  m6i.16xlarge box `hydrozoan-sim` (i-04c985d80ac6a03d2, 54.82.104.242, dead-man 8 h), configs
  `data/sim/batch1/`, results `results/sim-geo/batch1/` (seed 0, load 10 tx/s/validator,
  120 s). e2e p50 sim / AWS (10k): Graded x0 246/239, x8 429/401, x11 428/413, x12 664/644;
  Byz-only x0 245/240; Mysticeti x8 326/344, x12 546/540; Orcaella (8,3) x4 428/400, x11
  425/436; Hydrozoan (1,1,4) n=10 x1 221/223. Ratios 0.95-1.07, every fast-quorum and
  clock-quorum step at the AWS position (Graded fast path alive at x=11, dead at x=12;
  Mysticeti's Tokyo step between x=8 and x=12; Orcaella's at x=4). Leader p50 within 20 ms
  except n=10 (122 vs 142). Wall time on the box: 1-4 min per Hydrozoan/Orcaella run, 9.5 min
  Mysticeti x8 (committer cost, known issue). Mysticeti x0: 317/338 (wall 18.5 min on the loaded box). Author agreed the batch shows no bug;
  the remaining validation group (77 runs, seeds 0-1) started ~14:45 UTC on the same box.
  Paper: appendix head rewritten for the calibrated model, new `fig:sim-validation` (parity
  plot `figure_sim_validation`, data `data/evaluation/sim-geo-summary.csv` = pooled parse of
  the geo runs, kept in the paper repo so every figure is reproducible from it).

- Scope decisions 2026-09-18 ~14:45 UTC (author): the paper keeps Fig 12 (uniform crash sweep),
  Fig 13 (uniform equivocation) and Table V as they are, so the calibrated `equiv` and `sweep`
  groups are NOT run (configs generated and uploaded, harmless). Calibrated campaign = validation
  group (running) + heat map (449 unique cells x {x=0, x=f+c}; (0,0) has one run; started 14:45
  UTC on the same box with 64 slots alongside the 54 validation runs; ~215 MB RSS per sim). The
  parity plot (new Fig 11, `fig:sim-validation`) is the validation of the heat map.

- Incident 2026-09-18 ~15:00 UTC: with 118 simulations in parallel (54 validation + 64 heat
  map) the box's 250 GB root disk filled up: every n = 50 run keeps ~3.5 GB of WAL in unlinked
  temp files while it runs (freed at exit). Runs died with `assertion left == right` in
  `crates/dag/src/storage/wal.rs:172` (short write on ENOSPC): 54 of 77 validation runs and 127
  of the first 173 heat-map runs. Fix: root volume grown online to 1 TB (`modify-volume`,
  `growpart`, `resize2fs`; growpart only completed once the simulations were killed), then a
  retry set of 456 configs (every validation/heat-map config without `metrics-A.prom`) started
  at 16:15 UTC with 64 parallel (`results/retry` on the box). Sizing rule: ~3.5 GB disk and
  ~215 MB RAM per concurrent n = 50 simulation; 64 concurrent need ~230 GB free.
  Completed before the incident: 23 validation runs (small committees and short ones) and 46
  heat-map cells, kept and merged at parse time.

- Orcaella (5, 8) at n = 50, fault-free (2026-09-19 ~09:00 UTC, same deployment as the admitted
  09-19 runs): 254 / 350 ms at 10k, 298 / 425 ms at 100k (p50 / p90), block 140 / 167, slow
  share 1.0. 5f + 3c + 1 = 50 exactly, quorum 37. Identical to Orcaella (6, 6) (254 / 350 and
  296 / 417, quorum 38) within 2 ms p50: a quorum of 37 still sits inside the 42 nearby nodes
  and never waits for Tokyo, so fault-free latency does not move along the Orcaella (f, c)
  spectrum at this quorum location (the same reason Balanced, Crash-heavy and Blue Bottle
  coincide). Its Hydrozoan twin is (5, 8, 18) (p = 13, fast quorum 37 = quorum), not run for
  the same reason (8, 3, 19) and (16, 0, 0) were dropped. Use: third Orcaella point on the
  resilience-vs-latency figure; the live orchestrator p90 drifted to 520 ms in the last two
  scrapes of the 100k run, but the plateau p90 (425) is within 8 ms of Orcaella (6, 6).
  Files: `results/results-0669999/measurements-orcaella-l2-f5-c8-512-0-50-{10000,100000}.yaml`.

- Hydrangea, its own codebase (`asonnino/hydrangea` main, Fabric tooling), (f, c, k) = (6, 6, 14),
  p = 10, on the same 54 machines (2026-09-19 ~09:55 UTC, after the Orcaella (5, 8) runs; the
  very last run before the stop). Its `NodeParameters` require n = 3f + 2c + k + 1 exactly, so
  n = 45 (45 of the 54 machines, round-robin over the six regions in the settings order, so
  Tokyo has 7). Input 10k tx/s, 180 s (its own n = 50 files also use 180 s), timeout 1,000 ms,
  header 512 kB / 200 ms, block size 10 certificates, burst 50, `consensus_only=False`, all as
  in its `remote` task. Result: end-to-end median 47.6 s (mean 47.6 s), 4,691 tx/s; block
  commit 55 ms median to first commit (11 blocks/s, 1,916 blocks), header dispatch 104 ms from
  tx creation. Compare its own (9, 10, 2) at n = 50: 66.4 to 67.2 s at every load from 1k to
  100k. So the collapse is neither load-bound nor split-specific: the consensus layer commits
  blocks in ~55 ms while transactions take ~48 s end to end. Reviewer point only: goes in the
  Hydrangea table row / text, never as a plotted point (the paper's `figure_e1` filters
  `load_hydrangea(0, 50)`, so the n = 45 file does not enter any figure). Machine-image
  workarounds needed (nothing in Hydrangea changed): the bootstrap's `sysctl -p` needs an
  `/etc/sysctl.conf` that Ubuntu 26.04 lacks (created empty), and its pinned RocksDB 8.10 does
  not compile under g++ 15 without `<cstdint>` (`[env] CXXFLAGS = "-include cstdint"` in each
  machine's `~/.cargo/config.toml`). Note the bootstrap changes the machines' sysctl and
  `initcwnd`, so no Hydrozoan run may follow it on this testbed.
  Files: `results/hydrangea-2026-09-19/bench-0-45-1-True-10000-512.txt` (also in the paper's
  `data/evaluation/hydrangea/`); logs and `.committee.json` archived on the dev machine as
  `~/hydrangea-logs-2026-09-19.tar.gz`.

## 8. RTT matrix: what to measure and how (before the 2026-09-18 runs)

Purpose: the whole evaluation reasons in "nearby rounds" vs "rounds through Tokyo"; the paper
needs the measured round-trip times behind those words, per region pair and per instance (to
spot stragglers, which explain the slow-path shares of zero-spare configurations).

What to measure:

- All ordered pairs of the active testbed instances (54 after `start --instances 9`, i.e. 2862
  pairs), by public IP, ICMP `ping`, 20 probes per pair at 0.2 s spacing, 2 s per-probe timeout,
  both directions measured independently (asymmetry is itself a data point).
- Twice: once right after the instances are up and before the first benchmark, once after the
  last benchmark of the day. The two matrices bracket the campaign; a large difference between
  them is the jitter signal.
- Record per pair: min, avg, max, mdev (ms). Summarise per region pair by the median of the
  pair averages.

How to run (dev machine, `~/mysticeti`, tmux session `eval`):

```sh
# 1. Testbed up and complete: every region must list 9 active instances.
./target/release/replica remote-testbed --settings-path scripts/eval/settings-eval.yml status

# 2. ICMP must be allowed by the testbed security group in every region. If step 3 reports
#    "nan" for every pair, add the rule (group name = testbed_id) and re-run:
for r in us-east-1 us-east-2 eu-central-1 eu-west-2 eu-west-3 ap-northeast-1; do
  sg=$(aws ec2 describe-security-groups --region $r --filters Name=group-name,Values=hydrozoan-eval \
      --query 'SecurityGroups[0].GroupId' --output text)
  aws ec2 authorize-security-group-ingress --region $r --group-id "$sg" --protocol icmp \
      --port -1 --cidr 0.0.0.0/0
done

# 3. Measure (about one minute; every source pings in parallel).
scripts/eval/ping-matrix.sh scripts/eval/settings-eval.yml results/ping-matrix-$(date -u +%Y-%m-%dT%H%M).csv 20
```

Outputs: `results/ping-matrix-<time>.csv` with columns `src_region, src_ip, dst_region, dst_ip,
sent, received, min_ms, avg_ms, max_ms, mdev_ms` (a row with `nan` RTTs and `received` 0 means
ICMP is blocked or the instance is unreachable), and on stdout the region-to-region median
table plus the list of pairs that lost probes (paste both into the findings). Copy the CSV(s) to the paper's `data/evaluation/` and attach them
to the `eval-0669999` release with the day's measurements.

Sanity checks on the table: intra-region medians ~1 ms; us-east <-> eu ~70 to 90 ms; anything
<-> ap-northeast-1 ~110 to 230 ms (Tokyo is the tail from every other region); us-east-1 <->
us-east-2 ~12 ms. The "nearby round" of the text is the max over the nearby quorum of these
one-way delays (about half the RTT); a two-round fast commit through Tokyo should add roughly one
Tokyo RTT over a nearby one, which is the ~100 to 140 ms step seen in E3.

Stragglers: sort the per-pair CSV by `avg_ms` within each region pair; an instance whose RTTs are
consistently above its region's median is the candidate explanation for slow-path shares in
zero-spare runs (Byz-only at 100k, k = 10). Note its index in the orchestrator's order
(`status` lists instances per region in that order; the first 50 are the validators).
- 2026-09-18 morning (08:20 to 08:45 UTC): Byz-heavy (8, 3, 19) measured 267 / 368 ms at 10k
  (block 156) and 316 / 504 at 100k (block 173), fast share 1.0, all 54 instances back (9 per
  region). A same-hour Balanced (6, 6, 19) probe read 266 / 365 (block 152) against 255 / 352
  (block 137) the day before: the morning is +11 ms e2e / +15 ms block, systematically. Within
  the day Byz-heavy equals Balanced (267 vs 266), so the endpoint match holds for the third
  k = 2f + c configuration too, but the point cannot join yesterday's E1 figure; by the rule set
  the evening before, Byz-heavy, Crash-heavy, the Mysticeti end (16, 0, 0) and Graded 100k stay
  simulator-only and the E1 text says so. Today's files live in `results/results-0669999-day2/`
  (Byz-heavy) and `results/results-0669999-probe/` (Balanced probe) and carry their own `source`
  in `aws-summary.csv`.
- Closing RTT matrix 08:43 UTC (`results/ping-matrix-2026-09-18T0843.csv`): identical to the
  08:19 one within 2 ms on every region pair, 0 lost probes. So the +11 ms of the morning's
  protocol runs is not in the network RTT; the candidates are the instances themselves (after
  stop/start the orchestrator may pick a different subset and order of the 54, and NVMe/CPU
  state differs) or the load generator. Either way it is a per-deployment offset, which is why
  every plotted curve must come from one deployment, not just one build.
- 2026-09-18 10:05 UTC probe (second deployment of the day): Balanced (6, 6, 19) 10k read 259 /
  366 ms, block 145 / 240, vs 255 / 352, block 137 / 238 on 09-17 (+4 e2e, +8 block; the 08:39
  deployment was +11 / +15). RTT matrix `results/ping-matrix-2026-09-18T1004.csv`: region
  medians unchanged within 2 ms (Tokyo-Frankfurt route 227 vs 238), but 17 of 2862 pairs lost a
  probe (0 to 2 before). Per-node p50 floors shifted uniformly (min 242 -> 246, median 248 ->
  254, Tokyo nodes 296 -> 300): a deployment- or network-wide drift, not a different or slow
  machine (the 54 instances are the same EC2 instances; the orchestrator lists them in the
  API's order and takes the first 50 round-robin). Decision: no gap runs on this deployment;
  instances stopped; one more try ~13:00 UTC. E1 stays the 09-17 curves; text: Byz-heavy is
  Orcaella (8, 3) (261 at 10k, same deployment), Crash-heavy is Orcaella (2, 13)'s thresholds
  (simulator only), (16, 0, 0) is dropped (its unanimity fast rule is not Mysticeti's latency).
- 2026-09-18 12:00 UTC attempt: `InsufficientInstanceCapacity` for m5d.8xlarge in us-east-2,
  eu-west-2 and eu-west-3 (four tries over five minutes); only 27 instances (Tokyo, Frankfurt,
  N. Virginia) came up, so no probe was possible. Instances stopped again. Capacity, not
  jitter, is now the limiting factor for closing the E1 gap on this instance type.
- Decision 2026-09-18 12:10 UTC: one more attempt on the morning of 09-19 (fresh STS token,
  start 9 per region, RTT matrix, Balanced probe; on a pass only Crash-heavy (2, 13, 17) is
  run and joins the plotted set; otherwise E1 is final as the 09-17 curves). Then destroy.
- 2026-09-19 07:55 UTC: all 54 instances back (9 per region); RTT matrix
  `results/ping-matrix-2026-09-19T0755.csv` identical to 09-18 within 2 ms, 0 lost probes.
  Balanced probe 257 / 356 ms, block 145 / 239 (09-17: 255 / 352, block 137 / 238): e2e within
  2 ms, block +8 like the 09-18 10:05 deployment, so 09-17's block latency was the low end of
  the deployment spread. Judged a pass on the plotted quantity; Crash-heavy (2, 13, 17) run on
  this deployment and admitted to E1, with the +8 ms block variance noted here.
- 2026-09-19 08:05 to 08:30 UTC, deployment matching 09-17 on the plotted quantity: Crash-heavy
  (2, 13, 17) 255 / 351 ms at 10k (block 138 / 236) and 293 / 417 at 100k (block 167 / 253),
  fast share 1.0, no skips: on the endpoint curve to the millisecond (Balanced 255 / 296, block
  137 / 166); the fourth k = 2f + c split coincides as well. Graded (6, 8, 15) at 100k: 282 /
  408, block 154 / 214, fast 0.999: its E1 curve is complete and it is the lowest 100k point
  (fast quorum 39 inside the nearby set with three spare). Both admitted to E1 (files in
  `results/results-0669999/`).
