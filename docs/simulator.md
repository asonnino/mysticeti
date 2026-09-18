# Simulator

The `simulator` crate drives the protocols through a **discrete-event simulator**. All time is
simulated — no wall-clock delay — and everything runs in a single process against a seeded RNG, so
each run is fully reproducible. Unlike the [geo-replicated testbed](orchestrator.md), the simulator
isn't about measuring real wire performance; it's about exercising the protocols under
precisely-controlled network, load, and failure scenarios.

What's simulated and what's real:

- **Simulated**: time, the inter-replica network (uniform-random latency), and cryptography
  (disabled — signatures are skipped).
- **Real**: the `consensus` crate, the `replica` orchestration, the block pipeline, the commit rule,
  the WAL-backed storage (each replica gets its own temporary WAL-backed store, discarded at the end
  of the run), and the metrics stack. Faults you observe in a simulation run map directly onto faults
  in the actual protocol code.

## Running a Simulation

The entry point is the `simulate` subcommand of the `replica` binary. With no arguments it runs a
single simulation with the built-in defaults (committee of 10, 20 s, full mesh, Mysticeti):

```bash
cargo run --release simulate
```

To dump the built-in defaults as an annotated YAML starting point:

```bash
cargo run simulate --dump-config > my-sim.yaml
```

To run a custom config:

```bash
cargo run --release simulate --config-path my-sim.yaml
```

A single YAML file can hold either one simulation (a mapping) or a suite of simulations (a top-level
sequence). Each entry of a suite is a full `SimulationConfig`, so entries can independently vary any
parameter. Suites run sequentially and print a final summary table comparing every run.

See [`crates/simulator/examples/single.yaml`](../crates/simulator/examples/single.yaml) for the
annotated single-run template and
[`crates/simulator/examples/suite.yaml`](../crates/simulator/examples/suite.yaml) for a suite that
sweeps committee size, latency, topology, and protocol variant,
[`crates/simulator/examples/twin.yaml`](../crates/simulator/examples/twin.yaml) for an
equivocating-leader run, and
[`crates/simulator/examples/geography.yaml`](../crates/simulator/examples/geography.yaml) for a
geo-distributed committee.

## Configuration Reference

All fields are optional and fall back to the defaults shown.

| Field                               | Default                               | Description |
| ----------------------------------- | ------------------------------------- | ----------- |
| `name`                              | _(unset)_                             | Optional label shown in logs and the suite summary table. |
| `committee_size`                    | `10`                                  | Number of replicas. Stake is uniform. |
| `latency`                           | `uniform` over 50–100 ms              | Link latency model — see below. |
| `topology`                          | `fullMesh`                            | Network topology — see below. |
| `duration_secs`                     | `20`                                  | Simulated time for which to run the simulation. |
| `rng_seed`                          | `0`                                   | Seed for the deterministic RNG. Change this to get different per-run noise while keeping everything else fixed. |
| `replica_parameters`                | `ReplicaParameters::default()`        | Same tunables as a real replica: DAG round timeout, max block size, consensus protocol, leader count. |
| `load_generator`                    | `LoadGeneratorConfig::new_for_test()` | Built-in transaction generator (`load` tx/s, `transaction_size`, `initial_delay`). `null` for empty blocks. |
| `equivocating_leaders`              | `[]`                                  | Authority indices that behave as equivocating leaders — see below. |

## Link Latency

The `latency` field selects how long a message takes on each directed link. Every message is
delayed by its own latency, independently of the others in flight, up to a window of 1024
messages per directed link; past that, or when the receiver stops reading, the link stops taking
new messages and the sender waits. Links are FIFO (a message is never delivered before the
previous one on its link). Ranges are `start` inclusive and `end` exclusive; `start == end` is a
constant and `start > end` is rejected.

- **`uniform`:** every link draws uniformly from the same range.

  ```yaml
  latency:
    uniform:
      range_ms: {start: 50, end: 100}
  ```

- **`geographic`:** replicas are placed in regions and a message takes half the round-trip time
  between the two regions, plus a small uniform `extra_ms` (processing time and jitter, default
  0–1 ms). Authority `i` sits in `regions[i % regions.len()]`, which is the round-robin the
  orchestrator uses when it selects instances. A committee index therefore maps to the same region
  in the simulator and on the testbed, provided `regions` is in the order of the testbed's
  `settings.yml` and every region had an instance to give on each lap: the orchestrator silently
  skips a region that has run out, which shifts every later index. `rtt_ms[from][to]` is looked up
  per direction; a missing direction falls back to the reverse one and a missing intra-region
  entry is zero. Every pair of listed regions must resolve, and every listed RTT and `extra_ms`
  bound must be finite, non-negative and at most one hour, or the run is rejected before it starts.

  ```yaml
  latency:
    geographic:
      regions: [us-east-1, eu-west-2, ap-northeast-1]
      rtt_ms:
        us-east-1: {eu-west-2: 75.3, ap-northeast-1: 146.4}
        eu-west-2: {ap-northeast-1: 212.2}
      extra_ms: {start: 0, end: 1}
  ```

  [`examples/geography.yaml`](../crates/simulator/examples/geography.yaml) is a 50-replica,
  six-region committee calibrated from an RTT matrix measured on the AWS testbed.

The latency model composes with `topology` and `equivocating_leaders`: crashing a region is a
`partition` whose alive group leaves that region's indices out.

Earlier versions configured the uniform range with top-level `latency_min_ms` / `latency_max_ms`
keys. They are no longer read (unknown keys are ignored), so a config that still sets them runs
with the default latency: move them under `latency.uniform.range_ms`.

## Network Topologies

The `topology` field selects how the simulated network is wired at the start of the run (connections
are established once and held for the duration):

- **`fullMesh`** — every replica connects to every other. This is the happy path.
- **`oneDown: <index>`** — every replica except `<index>` is fully connected; `<index>` is isolated.
  Probes the `f = 1` failure mode.
- **`star: <center>`** — only `<center>` is connected to all other replicas; non-center replicas can
  only reach each other through the centre. Useful for studying bottlenecks and leader-fairness
  behaviour under a single hub.
- **`partition: [[...], [...], ...]`** — the committee is split into the listed groups; replicas
  only connect inside their own group. Lets you construct arbitrary network splits — two equal
  halves exercise "no quorum on either side" conditions.

## Byzantine Faults

The `equivocating_leaders` field turns the listed authorities into equivocating leaders. In
every round where such an authority holds a leader slot, it sends each peer two blocks for that
round: its real proposal and a *twin* with the same parents, transactions and timestamp but a
different digest (the last byte flipped). Odd-indexed peers receive the twin first, even-indexed
peers the original, so the committee's votes split between the two. Everything else the
authority does is honest.

The behaviour is a shim on the equivocator's outgoing links, so the replica code is unmodified.
The twin is also delivered back to the equivocator itself (as if a peer had sent it), so that
honest blocks referencing the twin stay causally complete for it and it keeps proposing.

What to expect: a slot with two twins cannot gather a fast quorum or a certificate for either,
so it is never fast- or slow-committed. Under a dual-path protocol (`dag-hydrangea`) each twin
still gathers a weak quorum of anchor-linked votes and the slot is committed by the weak rung of
the graded indirect rule, with the digest tie-break choosing the twin. Under a single-path
protocol (`mysticeti`) the slot is skipped indirectly. In neither case is a direct skip possible,
since every voter voted for one of the twins. The scenarios in
`crates/simulator/tests/dag_hydrangea.rs` and `crates/simulator/tests/simulation.rs` assert
exactly this through the `commit_type` breakdown of `committed_leaders_total`;
[`examples/twin.yaml`](../crates/simulator/examples/twin.yaml) is a ready-made run of the
DagHydrangea case whose exported `metrics-*.prom` files show the breakdown per leader.

Two approximations to keep in mind. The leader slots are computed from the protocol's leader
count and the round-robin `LeaderElector`, which is exact for the pipelined protocols; the
non-pipelined Cordial Miners variants equivocate in a few extra (non-leader) rounds, which is
harmless. And equivocating in every round is not offered: the leader rounds are where
equivocation bears on the commit rule.

## Outcomes

Every run ends with one of three verdicts, classified by comparing the committed-leader sequences of
every replica:

| Outcome      | Meaning                                                                                                                                                              |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Pass`       | Every replica's committed prefix agrees with the others' and at least one replica committed a leader.                                                                |
| `NoProgress` | Every replica's commits are consistent but none committed a leader. Expected under unrecoverable partitions (e.g. a symmetric split, or `star` on a crashed centre). |
| `Diverged`   | At least two replicas disagree on a commit within their shared prefix. This is a safety violation and fails the run.                                                 |

A suite's final exit code is non-zero if any simulation `Diverged`.

## Saving Detailed Results

Pass `--output-dir <DIR>` and the simulator drops a fixed set of artefacts on disk for each run.
The directory is created if it doesn't exist.

```bash
cargo run --release simulate \
    --config-path crates/simulator/examples/suite.yaml \
    --output-dir results/
```

For a single-run invocation the artefacts land directly in `<output-dir>/`. For a suite, each
simulation gets its own subdirectory named after the run (`name` field, sanitised) or by 1-based
index when unnamed. Files are written atomically (`NamedTempFile` + `persist`), so a mid-write crash
leaves each file either intact or absent — never half-written.

| File                       | Contents                                                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<output-dir>/tracing.log` | Suite-wide tracing log (one log for the whole invocation).                                                                                                |
| `<run>/config.yaml`        | The full `SimulationConfig` that produced the run, including any defaults filled in.                                                                      |
| `<run>/meta.yaml`          | `{ outcome, duration_secs, kind: simulation, timestamp_unix }`.                                                                                           |
| `<run>/metrics-<replica>.prom` | One Prometheus text exposition per replica (`metrics-A.prom`, `metrics-B.prom`, …), each self-contained and parseable by `promtool` and most TSDB ingesters. |
| `<run>/dag.ndjson`         | _(opt-in via `--export-dag`)_ every committed sub-DAG, one JSON object per line. Can be many GB; off by default.                                          |

## Programmatic Use

The simulator is also usable as a library, which is how the integration tests in
[`crates/simulator/tests/simulation.rs`](../crates/simulator/tests/simulation.rs) drive it. A
minimal example:

```rust
use replica::result::Outcome;
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

#[test]
fn one_down_stays_consistent() {
    let config = SimulationConfig {
        committee_size: 7,
        topology: NetworkTopology::OneDown(0),
        duration_secs: 40,
        ..Default::default()
    };

    let result = SimulationRunner::new(config).run().expect("simulation");

    assert_ne!(result.outcome, Outcome::Diverged);
    assert!(!result.metrics.is_empty());
}
```

`SimulationRunner::run` returns an `io::Result<RunResult<SimulationConfig>>` carrying per-replica
`MetricsSnapshot`s, the derived `Outcome`, the per-replica storages, and the original config.
`SimulationRunner::from_yaml` loads a `SimulationConfig` from disk, useful when test cases share
configuration with the CLI. The committed sub-DAGs stay available through the returned storages —
that is how the CLI's `--export-dag` flag produces `dag.ndjson` after the run. Because every run is
deterministic in the `rng_seed`, these tests are reproducible across machines.

For finer control than `SimulationRunner` offers — external transaction submission via
`TransactionClient` and live commit streaming via `ReplicaBuilder::with_commit_consumer` —
tests can build the committee directly with `SimulatedNetwork::new_for_test` (or
`new_for_test_with_commit_consumers`), as
[`crates/simulator/tests/commit_consumer.rs`](../crates/simulator/tests/commit_consumer.rs) does.
One rule applies: **any task that awaits replica channels (submitters, commit collectors) must run
inside the simulated world**, spawned via `SimulatorContext::spawn` — awaiting from outside the
executor deadlocks the run. Determinism extends to the consumer streams: identical seeds yield
byte-identical commit sequences.
