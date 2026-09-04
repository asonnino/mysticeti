# Steelhead experiment pipeline

Runs the paper's simulation campaigns and turns the results into figures and
numbers. Everything is driven by the experiment matrix in
`steelhead_exp/matrix.py`; run artefacts are cached under `data/steelhead/`,
figures and the fillins report land in `plots/` (both gitignored).

## Setup

```sh
cargo build --release -p cli          # the simulator binary
python3 -m venv scripts/.venv
scripts/.venv/bin/pip install -r scripts/requirements.txt
```

## Workflow

Run from the repository root:

```sh
scripts/.venv/bin/python scripts/steelhead_exp status                 # matrix coverage: done / missing / failed
scripts/.venv/bin/python scripts/steelhead_exp run                    # run everything missing (parallel, cached)
scripts/.venv/bin/python scripts/steelhead_exp run --filter 'smoke*'  # stage a subset (fnmatch on job names)
scripts/.venv/bin/python scripts/steelhead_exp plot                   # all figures from cached data (never simulates)
scripts/.venv/bin/python scripts/steelhead_exp fillins                # plots/fillins.md with the paper's numbers
```

`run` skips any job whose output dir holds a `meta.yaml` with `outcome: pass`,
so plot iterations never re-simulate; failed or partial runs are wiped and
retried. Failures are recorded (per-job `runner-error.log`) and summarized,
never fatal. `--workers N` bounds the process pool (default: cores − 2).

Job names encode the parameters (`good--n10--mm--sh-p4--L1000--s0`), but the
authoritative parameter record is the matrix itself; analysis iterates the
matrix and reads each job's directory.
