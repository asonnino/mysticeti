<p align="center">
  <img src="assets/banner.png" alt="Mysticeti" width="720" />
</p>

# Mysticeti

[![build
status](https://img.shields.io/github/actions/workflow/status/asonnino/mysticeti/code.yaml?branch=main&logo=github&style=flat-square)](https://github.com/asonnino/mysticeti/actions)
[![rustc](https://img.shields.io/badge/rustc-1.92+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repository provides reference implementations of several uncertified DAG-based consensus
protocols. The codebase is designed to be small, modular, and easy to benchmark and modify. It is
not intended for production use, but relies on real networking, cryptography, and a persistent
storage. It also ships with a discrete-event simulator for exercising the protocols under custom
network and failure scenarios.

This repository currently supports [Mysticeti](https://sonnino.com/papers/mysticeti.pdf),
[Mahi-Mahi](https://sonnino.com/papers/mahi-mahi.pdf),
[Odontoceti](https://sonnino.com/papers/bluebottle.pdf), [Cordial
Miners](https://arxiv.org/abs/2205.09174) (both the partially synchronous and asynchronous
variants), and [Nemo-Nemo](https://sonnino.com/papers/nemo-nemo.pdf).

## Quick Start

To boot a local testbed of 4 replicas with a built-in load generator on your machine:

```
$ git clone https://github.com/asonnino/mysticeti.git
$ cd mysticeti
$ cargo run local-testbed
```

The testbed runs for 20 seconds by default and prints a per-replica summary on exit. Pass
`--duration <SECS>` to change the run length, or `--perpetual` to keep it running until `Ctrl-C`
(and print the summary on exit). Pass `--output-dir <DIR>` to also write the run's configuration,
Prometheus metrics, and tracing log under `<DIR>`.

```text
┌                                                                                       ┐
│                                                                                       │
│ Mysticeti (2 leaders/round)                                                           │
│ Mode: Local Testbed                                                                   │
│ Replicas: 4                                                                           │
│ Tx size: 32 bytes                                                                     │
│ Load: 40 tx/s                                                                         │
│                                                                                       │
└                                                                                       ┘

Running for 20 seconds…
[t=   5s] · committed= 10k · commits/s=  2k · tx/s=  40 · p50=  50ms · p90=  90ms
[t=  10s] · committed= 20k · commits/s=  2k · tx/s=  40 · p50=  50ms · p90=  90ms
[t=  15s] · committed= 31k · commits/s=  2k · tx/s=  40 · p50=  50ms · p90=  90ms

✓ Commits consistent across all replicas
╭─────────┬───────────────────┬───────────┬──────┬─────────────┬─────────────┬──────────╮
│ replica │ committed leaders │ commits/s │ tx/s │ p50 latency │ p90 latency │ timeouts │
├─────────┼───────────────────┼───────────┼──────┼─────────────┼─────────────┼──────────┤
│ A       │ 41794             │ 2090.5    │ 40   │ 50 ms       │ 90 ms       │ 1        │
│ B       │ 41794             │ 2090.5    │ 40   │ 50 ms       │ 90 ms       │ 1        │
│ C       │ 41796             │ 2090.6    │ 40   │ 50 ms       │ 90 ms       │ 1        │
│ D       │ 41796             │ 2090.6    │ 40   │ 50 ms       │ 90 ms       │ 1        │
╰─────────┴───────────────────┴───────────┴──────┴─────────────┴─────────────┴──────────╯
```

## Next Steps

- [Code architecture](docs/architecture.md) — how the system is put together, from threading and
  storage to where cryptography sits on the hot path.
- [Simulator](docs/simulator.md) — driving the protocols through custom network, load, and failure
  scenarios with the discrete-event simulator.
- [Geo-replicated testbeds](docs/orchestrator.md) — a walkthrough for spinning up and benchmarking a
  distributed deployment on the cloud.

## License

This software is licensed as [Apache 2.0](LICENSE).
