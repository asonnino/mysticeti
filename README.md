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
[Odontoceti](https://sonnino.com/papers/bluebottle.pdf), and [Cordial
Miners](https://arxiv.org/abs/2205.09174) (both the partially synchronous and asynchronous
variants).

## Quick Start

To boot a local testbed of 4 replicas with a built-in load generator on your machine:

```
$ git clone https://github.com/asonnino/mysticeti.git
$ cd mysticeti
$ cargo run --release --bin replica -- local-testbed
```

The first build may take a few minutes. Each replica exposes Prometheus metrics on its own local
port; press `Ctrl-C` to stop the testbed.

## Next Steps

- [Code architecture](docs/architecture.md) — how the system is put together, from threading and
  storage to where cryptography sits on the hot path.
- [Simulator](docs/simulator.md) — driving the protocols through custom network, load, and failure
  scenarios with the discrete-event simulator.
- [Geo-replicated testbeds](docs/orchestrator.md) — a walkthrough for spinning up and benchmarking a
  distributed deployment on the cloud.

## License

This software is licensed as [Apache 2.0](LICENSE).
