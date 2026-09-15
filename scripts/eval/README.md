# Evaluation plots

Plotting pipeline for the Orcaella / Hydrozoan evaluation figures, ported from the
Orcaella paper repo (`hybrid-fault-tolerance/data/plot.py`). It parses the
orchestrator's `measurements-*.yaml` Prometheus dumps under `results/` and writes
every figure (pdf + png) to `plots/` at the repo root (gitignored).

## Layout

- `plot.py` — the whole pipeline; `python scripts/eval/plot.py` drives every figure.
- `hydrangea/bench-*.txt` — Hydrangea text summaries (different format, copied from
  the paper repo; the Hydrangea line is illustrative and off-scale at every load).
- `requirements.txt` — `matplotlib`, `numpy`, `pyyaml`.

Input data lives in `results/results-<commit>/<testbed>/measurements-*.yaml`; the
script currently reads `results/results-96dee8d/{eu-us,global,eu}`. New campaigns
should land in a new `results/results-<commit>/<testbed>/` directory and be wired
in through the `RESULTS*` constants at the top of `plot.py`. Never edit files under
`results/` from this pipeline: the data is read-only.

## Running

```sh
python3 -m venv scripts/.venv-eval
scripts/.venv-eval/bin/pip install -r scripts/eval/requirements.txt
MPLCONFIGDIR=$(mktemp -d) scripts/.venv-eval/bin/python scripts/eval/plot.py
```

`MPLCONFIGDIR` only matters when `~/.matplotlib` is not writable (sandboxes);
otherwise a plain `python scripts/eval/plot.py` works from any cwd. The venv at
`scripts/.venv-eval/` is gitignored.

Two data sets used by the paper are not tracked here, and the script degrades
cleanly without them:

- `results/results-2a2bc14/` (checkpoint-instrumented campaign): when absent,
  `happy_case_large`, `latency_bars_*` and `improvement_vs_load` fall back to the
  original campaign and `checkpoint_happy_case` / `checkpoint_bars_*` are skipped.
- `results/results-96dee8d/global/mysticeti-c-new/` (Mysticeti re-run on the global
  testbed): when absent, `happy_case_global` / `latency_bars_global_*` are skipped.

## Filename to config decoding

- Orcaella: `measurements-orcaella-l2-f{F}-c{C}-512-{faults}-{nodes}-{load}.yaml`
- Mysticeti: `measurements-mysticeti-l2-512-{faults}-{nodes}-{load}.yaml`
- Hydrangea: `bench-{faults}-{nodes}-{x}-True-{load}-512.txt`
- Runs with `crash_order: region-order` carry a `-region-order` suffix after `{faults}`.
  The loaders only accept files whose order matches `plot.CRASH_ORDER` (default
  `round-robin`), so the two orders never mix in one figure.
- `512` is the transaction size in bytes, `faults` the number of crashed nodes,
  `load` the offered tx/s.
- Orcaella with `c=0` is Blue Bottle (the protocol collapses to 5f+1). By default it
  is labelled as an ordinary Orcaella config; flip `BLUEBOTTLE_AS_BASELINE` in
  `plot.py` to present it as a separate baseline.
- Mysticeti is 3f+1 (n=49 means f=16; n=10 means f=3; n=7 means f=2).
- Hydrangea is 3f+2c+k+1; the n=50 run is labelled f=9,c=10,k=2.

## Metric extraction (gotchas)

The YAMLs are PromQL-windowed (`rate(..[60s])`, `histogram_quantile(..)`) and
averaged across node-series. Every node commits the full total order, so each
node's `latency_s_count` rate is the cluster throughput.

- `parse_yaml` aggregates over the throughput plateau (scrapes with
  `tps >= 0.9 * max`) and reports min p50 / min p90 with the median throughput. Do
  not use the single latest scrape; end-of-run spikes inflate it.
- Scrape timestamps are rounded to the nearest second before joining series.
- Mean (`sum/count`) and stdev (from `latency_squared_s`) come from raw accumulators
  and are accurate at any latency. Percentiles come from a bucketed histogram and
  pin to bucket edges (50/90/99 ms) when latency is close to the first bucket; the
  intra-EU crash-only run (~22 ms) uses mean and stdev for that reason.

### Metrics not yet parsed

PR #269 added two metrics the current `plot.py` does not read:

- `committed_leaders_total{commit_type}` with `commit_type` in `fast-commit`,
  `slow-commit`, `indirect-commit-certificate`, `indirect-commit-weak`,
  `direct-skip`, `indirect-skip` (single-path protocols only ever emit `slow-commit`
  and `indirect-commit-certificate`).
- `block_latency_s` (with `block_latency_squared_s`), labelled by `kind`.

Hydrozoan fast-path / slow-path breakdown figures will need new loaders for these.

## Testbeds

- `eu-us/` is canonical. Despite the name it spans 6 AWS regions including Tokyo,
  giving a bimodal latency (US/EU cluster ~290-320 ms, ~9-node Tokyo tail ~355 ms).
  Holds the full 50-node no-fault sweep and the fault runs.
- `global/` is appendix material.
- `eu/` is the intra-Europe crash-only run (f=0, c=1, n=3, ~22 ms), two loads only.

## Figures

- `happy_case_large` — latency-throughput, ~50 nodes, no faults, eu-us. p50 line
  plus p90 upper whisker.
- `param_coverage` — parameter coverage of the measured configs.
- `fault_coverage` — (f,c) plane: Orcaella coverage (5f+3c<=48) vs the Mysticeti
  boundary (f+c=16).
- `latency_bars_{10,50,100}k` — per-protocol p50 bars at one load: `50k` with a
  single gap arrow, `10k` with placement-stdev error bars and quorum annotations,
  `100k` stacked base(@10k) + queuing increment.
- `fault_tradeoff_{10k,50k,100k}` — (f,c) plane with the measured configs colored
  by % improvement over Mysticeti; shared color scale across loads.
- `improvement_vs_load` — % improvement over Mysticeti vs load, one line per
  Orcaella config.
- `geo_boundary_bars` — Mysticeti f=3 (n=10) vs Orcaella f1c1 (n=9) vs Mysticeti f=2
  (n=7): quorum size bites when it forces the slow region in.
- `faults_small_eu_us` — latency-throughput under 2 crash faults, small committees.
- `crash_only_eu` — box plot (mean +-1 std, +-2 std whiskers) of the EU crash-only
  run, in ms.
- `checkpoint_happy_case`, `checkpoint_bars_*`, `happy_case_global`,
  `latency_bars_global_*` — need the untracked data sets listed above.

Colors: Blue Bottle blue, Orcaella green, Mysticeti orange, Hydrangea red.
Latency-throughput legends use a fixed order (Hydrangea, Mysticeti, Blue Bottle,
Orcaella) via `_ordered_handles`.
