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

## Experiment: Mysticeti with a 4n/5 parent quorum (branch `exp/mysticeti-wide-parents`)

> [!WARNING]
> **Never merge this branch.** `mysticeti-wide-parents` is not a real protocol. It is Mysticeti
> with blocks waiting for `4n/5 + 1` parents instead of `2n/3 + 1`. It keeps Mysticeti's safety
> (its commit, skip and certificate quorums are unchanged), but it is **not live at the 3f+1
> bound**: it stalls once more than `n/5` replicas are down (9 at n = 46), against Mysticeti's
> 15. It exists only to run the ablation below. Its data is kept reproducible by the
> `eval-<sha>` tag on this branch's commit, not by merging.

Under jitter, the 5f+1 protocols (e.g. Blue Bottle PS) beat the 3f+1 ones (e.g. Mysticeti). Two
effects could explain it:

1. **Wider parent sets:** waiting for `4n/5` parents instead of `2n/3` puts more of the
   previous round, and so more votes for the leader, under each block.
2. **The commit rule's shape:** 4f+1 direct links to the leader within one round (wave length
   2) may be likelier than 2f+1 certificates over two rounds (wave length 3).

`mysticeti-wide-parents` changes only effect (1): the parent quorum (`quorum_threshold`) matches
Blue Bottle PS (37 of 46), and everything else is Mysticeti's. If it moves towards Blue Bottle
PS, effect (1) dominates. If it stays with Mysticeti, apart from slower rounds, effect (2) does.
The experiment applies only to wave-length-3 (partially synchronous) protocols: Blue Bottle's
indirect rule relies on its `4n/5` parents, so it can't run the opposite way.

The `wide-parents` experiment in `matrix.py` runs `mysticeti-wide`, `mysticeti` and `bb-ps`,
with and without jitter, at 0, 4 and 9 crashes (9 is the most the variant tolerates), over
7 seeds: 126 runs. Run only those runs, from this branch's build, into a fresh directory:

```sh
cargo build --release --bin replica
python3 scripts/sok_eval/run.py --binary target/release/replica --out results/wide-parents \
    --experiment wide-parents --workers 60
python3 scripts/sok_eval/summary.py --out results/wide-parents --experiment wide-parents
```

The baselines run again on purpose. Results from different builds are never compared or
released together, and these runs reuse the main campaign's names, so they need their own
`--out`.
