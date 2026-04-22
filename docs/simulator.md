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
  the WAL-backed storage (each replica gets its own ephemeral WAL directory, discarded at the end of
  the run), and the metrics stack. Faults you observe in a simulation run map directly onto faults
  in the actual protocol code.

## Running a Simulation

The entry point is the `simulate` subcommand of the `replica` binary. With no arguments it runs a
single simulation with the built-in defaults (committee of 10, 20 s, full mesh, Mysticeti):

```bash
cargo run --release --bin replica -- simulate
```

To dump the built-in defaults as an annotated YAML starting point:

```bash
cargo run --bin replica -- simulate --dump-config > my-sim.yaml
```

To run a custom config:

```bash
cargo run --release --bin replica -- simulate --config-path my-sim.yaml
```

A single YAML file can hold either one simulation (a mapping) or a suite of simulations (a top-level
sequence). Each entry of a suite is a full `SimulationConfig`, so entries can independently vary any
parameter. Suites run sequentially and print a final summary table comparing every run.

See [`crates/simulator/examples/single.yaml`](../crates/simulator/examples/single.yaml) for the
annotated single-run template and
[`crates/simulator/examples/suite.yaml`](../crates/simulator/examples/suite.yaml) for a suite that
sweeps committee size, latency, topology, and protocol variant.

## Configuration Reference

All fields are optional and fall back to the defaults shown.

| Field | Default | Description |
| --- | --- | --- |
| `name` | *(unset)* | Optional label shown in logs and the suite summary table. |
| `committee_size` | `10` | Number of replicas. Stake is uniform. |
| `latency_min_ms` / `latency_max_ms` | `50` / `100` | Inclusive range for per-message link latency, sampled uniformly for every delivery. |
| `topology` | `fullMesh` | Network topology — see below. |
| `duration_secs` | `20` | Simulated time for which to run the simulation. |
| `rng_seed` | `0` | Seed for the deterministic RNG. Change this to get different per-run noise while keeping everything else fixed. |
| `replica_parameters` | `ReplicaParameters::default()` | Same tunables as a real replica: DAG round timeout, max block size, consensus protocol, leader count. |
| `load_generator` | `LoadGeneratorConfig::new_for_test()` | Built-in transaction generator (`load` tx/s, `transaction_size`, `initial_delay`). Set to `null` to run with empty blocks only. |

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

## Outcomes

Every run ends with one of three verdicts, classified by comparing the committed-leader sequences of
every replica:

| Outcome | Meaning |
| --- | --- |
| `Pass` | Every replica's committed prefix agrees with the others' and at least one replica committed a leader. |
| `NoProgress` | Every replica's commits are consistent but none committed a leader. Expected under unrecoverable partitions (e.g. a symmetric split, or `star` on a crashed centre). |
| `Diverged` | At least two replicas disagree on a commit within their shared prefix. This is a safety violation and fails the run. |

A suite's final exit code is non-zero if any simulation `Diverged`.

## Saving Detailed Results

`--results-file <path>` writes a machine-readable report per simulation. The output format is
inferred from the extension (`.json`, `.yaml`, or `.yml`). For a suite, the file contains one
`SimulationReport` per simulation, in run order.

Each report contains:

- The full `SimulationConfig` that produced the run (including any defaults filled in).
- The `Outcome` and a `commits_consistent` flag.
- `duration_secs` of the run.
- Per-replica entries with `authority`, `committed_leaders` (formatted `"A<round>"`, e.g. `"3-17"`),
  `commits`, `commits_per_sec`, aggregated `ReplicaStats`, and the full per-replica metrics as a
  Prometheus text-exposition string. The Prometheus blob is parseable by `promtool`, Prometheus
  itself, and most TSDB ingesters — so a simulation can be post-hoc dropped into the same tooling
  used for real runs.

```bash
cargo run --release --bin replica -- simulate \
    --config-path crates/simulator/examples/suite.yaml \
    --results-file suite-results.json
```

## Programmatic Use

The simulator is also usable as a library, which is how the integration tests in
[`crates/simulator/tests/simulation.rs`](../crates/simulator/tests/simulation.rs) drive it. A
minimal example:

```rust
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

#[test]
fn one_down_still_commits() {
    let config = SimulationConfig {
        committee_size: 7,
        topology: NetworkTopology::OneDown(0),
        duration_secs: 40,
        ..Default::default()
    };

    let results = SimulationRunner::new(config).run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
}
```

`SimulationRunner::run` returns a `SimulationResults` with per-replica committed leaders,
per-replica `MetricsSnapshot`s, and the `commits_consistent` flag. `SimulationRunner::from_yaml`
loads a `SimulationConfig` from disk, useful when test cases share configuration with the CLI.
Because every run is deterministic in the `rng_seed`, these tests are reproducible across machines.
