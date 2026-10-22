# lean-dag SoK simulation campaign

Simulator campaign comparing every protocol the unified decider produces (n = 46). Plain
Python 3, no third-party modules.

- `matrix.py`: the configurations and the five experiments (happy case, crash sweep,
  Byzantine faults, jitter, buffer rounds). Jobs are named by content, so a run that two
  experiments need runs once.
- `run.py`: runs one shard of the matrix in parallel, skipping runs that already finished.
- `summary.py`: one CSV per experiment, measured from 30 s to the end over live replicas.

```sh
cargo build --release --bin replica
python3 scripts/sok_eval/run.py --binary target/release/replica --out results/sok \
    --shard 0/4 --workers 60
python3 scripts/sok_eval/summary.py --out results/sok
```

Point `TMPDIR` at a fast local disk: each run's ephemeral WALs take a few GB. Raw results go
to `results/` (gitignored) and are published as a release, as described in CLAUDE.md.
