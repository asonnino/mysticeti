# Architecture

This document describes how a replica is put together and runs. It's aimed at readers who already
know *what* the protocol does (see the paper links in the [README](../README.md)) and want to know
*how* the code implements it.

## Crates

The workspace is split along the core architectural seam: the DAG layer and the consensus layer are
separate crates, with a narrow trait boundary between them.

| Crate | Role |
| --- | --- |
| [`dag`](../crates/dag) | Block, committee, and crypto primitives; the block pipeline (`Core` + `Syncer`); networking; the WAL. Defines the `DagConsensus` seam. |
| [`consensus`](../crates/consensus) | The committer and all protocol variants. Implements `DagConsensus`. Depends only on `dag`. |
| [`replica`](../crates/replica) | Wires a runnable node: builds storage, crypto, the block handler, the core, and the networking. The built-in benchmark load generator also lives here. |
| [`cli`](../crates/cli) | The `replica` binary — subcommands for genesis, running a node, spinning up a local testbed, and driving the simulator. |
| [`simulator`](../crates/simulator) | Discrete-event simulator (see [`docs/simulator.md`](simulator.md)). Runs the real `dag` and `consensus` crates against a simulated clock and network. |
| [`orchestrator`](../crates/orchestrator) | Geo-distributed benchmark driver (see [`docs/orchestrator.md`](orchestrator.md)). |
| [`minibytes`](../crates/minibytes) | Zero-copy byte buffer type (vendored from Facebook's Sapling). Lets `dag` hand out `&[u8]` views over memory-mapped WAL segments without copying. |

### The `dag` ↔ `consensus` interface

The two crates talk through the [`DagConsensus`](../crates/dag/src/consensus.rs) trait, which is
deliberately small:

```rust
pub trait DagConsensus: Send + 'static {
    fn quorum_threshold(&self) -> Stake;
    fn try_commit(&mut self, last_decided: Option<(RoundNumber, Authority)>)
        -> impl Iterator<Item = LeaderStatus>;
    fn get_leaders(&self, round: RoundNumber) -> Option<impl Iterator<Item = Authority>>;
}
```

`try_commit` is the commit rule of the consensus protocol. It is called by the DAG core whenever
enough new blocks have accumulated to potentially extend the committed sequence, and it is
idempotent: given the same DAG state and the same `last_decided` anchor, it returns the same
sequence of `LeaderStatus` decisions, so repeated invocations are safe and cheap. `get_leaders` lets
the DAG consult the commit rule when deciding whether a new block can be safely proposed.
`consensus` owns no state machine of its own — it reads the DAG through a borrowed `BlockReader` and
returns decisions. The [`Committer`](../crates/consensus/src/committer.rs) in `consensus` is the
only `DagConsensus` implementation shipped today; protocol variants differ only in the parameters
they pass to it.

## Anatomy of a Replica

A running replica mixes three concurrency domains: a dedicated OS thread that owns all mutable
consensus state, a tokio runtime that owns I/O, and a second OS thread that fsyncs the WAL in the
background.

```
┌────────────────────────────── Replica ───────────────────────────────┐
│                                                                      │
│   tokio runtime                        dag thread (std::thread)      │
│  ┌─────────────────────┐              ┌───────────────────────────┐  │
│  │  accept task        │              │  Core + Syncer            │  │
│  │  per-peer send      │  CoreThread  │  · threshold clock        │  │
│  │  per-peer recv      │─────cmds────▶│  · pending blocks         │  │
│  │  block fetcher      │◀────blocks───│  · new proposals          │  │
│  │  round timeout      │              │  · commit pipeline        │  │
│  │  cleanup tick       │              └──────────────┬────────────┘  │
│  │  load generator     │                             │ writes        │
│  │  metrics server     │                             ▼               │
│  └──────────┬──────────┘                 ┌─────────────────────────┐ │
│             │ reads committed            │   Write-Ahead Log       │ │
│             │ blocks & metrics           │   blocks │ payloads │    │ │
│             └───────────────────────────▶│          commits        │ │
│                                          └────────────▲────────────┘ │
│                                                       │ fsync        │
│                                          wal-syncer thread (std)     │
└──────────────────────────────────────────────────────────────────────┘
```

The only mutable DAG state — the `Core` — lives on the `dag` thread. Tokio tasks never touch it
directly: they send typed commands over an mpsc channel and await a `oneshot` reply. Every invariant
the consensus relies on (threshold clock monotonicity, block-insertion ordering, commit linearity)
is single-threaded by construction.

## The Sync Core

The core is spawned as a named OS thread — not a tokio task — in
[`crates/dag/src/core/core_thread.rs`](../crates/dag/src/core/core_thread.rs):

```rust
thread::Builder::new().name("dag").spawn(move || core_thread.run())
```

Inside that thread, `CoreThread::run` is a `blocking_recv()` loop over an mpsc channel of
`CoreThreadCommand` messages (capacity 32):

- **`AddBlocks(blocks)`** — incoming blocks, after signature verification on the tokio side. The
  core inserts them into the block store, advances the threshold clock, and may produce a new
  proposal.
- **`ForceNewBlock(round)`** — sent by the round-timeout task when the timer fires. The core
  produces a proposal even without a complete quorum.
- **`Cleanup`** — periodic, trims committed state.
- **`GetMissing`** — returns the set of references the DAG is waiting for, used to drive block-fetch
  requests.
- **`ConnectionEstablished(authority)` / `ConnectionDropped(authority)`** — liveness hints; the core
  uses these when deciding whether a round is ready to advance (see [Protocol-Conditional
  Behaviour](#protocol-conditional-behaviour)).

Running the core synchronously — rather than as an async task — avoids task-scheduling jitter on the
hot path (a block insert is a handful of hash-table operations and should finish in microseconds),
keeps every data structure lock-free, and makes the loop trivially reproducible. The tokio side
provides concurrency *around* the core, not inside it.

## Networking

Networking lives entirely on the tokio side, under [`crates/dag/src/sync/`](../crates/dag/src/sync).
The `NetworkSyncer` is the top-level controller; its `start` method spawns the long-lived tasks
described below. Frames on the wire are `u32 length || bincode-serialized NetworkMessage` (max 16
MB); one `Connection` is held per peer, each with separate send and receive mpsc channels (capacity
16 each).

### Tasks spawned per replica

All of these live in [`crates/dag/src/sync`](../crates/dag/src/sync):

| Task | Owner | Responsibility |
| --- | --- | --- |
| **Accept task** | `Server::run` (`network.rs`) | Single TCP listener. Hands each inbound socket to a worker for handshake and `Connection` setup. |
| **Outbound worker** | `Worker::run` (`network.rs`) | One per remote authority. Runs the handshake, retries on failure, and staggers startup to avoid dial storms. |
| **Per-peer read task** | `Worker::handle_read_stream` | Deserializes incoming `NetworkMessage`s and forwards them to the owning `connection_task`. |
| **Per-peer write task** | `Worker::handle_write_stream` | Serializes outbound messages from the send channel and frames them. |
| **`connection_task`** | `NetworkSyncer::connection_task` (`net_sync.rs`) | One per peer. Verifies each `Block` on the tokio task, forwards to the core. Also handles sync requests. |

The handshake uses the constants `ACTIVE_HANDSHAKE` / `PASSIVE_HANDSHAKE`; nodes with `id < our_id`
dial immediately, the rest wait a short random delay before dialling, to avoid simultaneous reconnect
storms.

### Tasks spawned per peer connection

| Task | Owner | Responsibility |
| --- | --- | --- |
| **`stream_own_blocks`** | `BlockDisseminator` (`synchronizer.rs`) | Streams this replica's own blocks to the peer, starting from the round the peer subscribed to. |
| **Others' blocks streams** | `BlockDisseminator` | Up to two concurrent streams per peer for blocks authored by other validators that the peer is missing. |

### Replica-wide background tasks

| Task | Spawn site | Responsibility |
| --- | --- | --- |
| **Round-timeout task** | `net_sync.rs` | Sleeps for `round_timeout`. On fire, sends `ForceNewBlock` to the core. Rearmed via a `Notify` whenever the core advances the round. |
| **Cleanup task** | `net_sync.rs` | Every 10 s, sends a `Cleanup` command to the core. |
| **Block fetcher** | `BlockFetcher::start` (`synchronizer.rs`) | Queries the core for missing block references and issues `RequestBlocks` to peers likely to have them. |
| **Load generator** | `TransactionGenerator::start` (`replica/src/generator.rs`) | Optional. Emits synthetic transactions at a configured rate; the first 8 bytes carry a timestamp for latency. |
| **Metrics server** | Prometheus server, injected via the builder | Exposes a `/metrics` endpoint for each replica. |

### From tokio to the core, and back

Any tokio task that needs to mutate consensus state calls an async method on `Syncer`, which sends a
`CoreThreadCommand` and awaits a `oneshot` reply (see
[`core/core_thread.rs`](../crates/dag/src/core/core_thread.rs)). Backpressure is implicit: if the
core thread is slow, the command channel (capacity 32) fills up, and the calling task's `.await`
stalls — which propagates naturally to the networking buffers (capacity 16 per connection) and
ultimately to the TCP socket.

## Storage: the Custom WAL

Storage is a purpose-built write-ahead log — not RocksDB, not a B-tree, not a database at all. It
lives in [`crates/dag/src/storage`](../crates/dag/src/storage), with the mmap byte abstraction
provided by [`minibytes`](../crates/minibytes). The design of this WAL later inspired the
[Tidehunter](https://sonnino.com/papers/tidehunter.pdf) database. The on-wire format of a single
entry is:

```
┌──────┬────────┬─────┬──────────────┐
│ crc32│ length │ tag │   payload    │
│  u32 │  u64   │ u32 │  bincode     │
└──────┴────────┴─────┴──────────────┘
```

Three tag kinds cover everything the node persists: blocks (the canonical `Data<Block>` form that's
also sent on the wire), payloads (the transaction batches produced by the load generator and pending
inclusion in a future proposal), and commits (the `CommitData` emitted by the committer — leader
plus ordered sub-DAG). No separate "metadata" store: the WAL is the only source of truth.

Reads are zero-copy. The file is memory-mapped in 256 MB segments, and `BlockReader` hands out
`minibytes::Bytes` slices that point directly into the mmap — no deserialization until something
actually asks for a field.

**Write path.** Only the `dag` thread ever writes. When a block arrives, the core's `add_blocks`
path serializes it and appends a `WAL_ENTRY_BLOCK`. When the load generator hands in a fresh
transaction batch, it becomes a `WAL_ENTRY_PAYLOAD` before the block handler holds on to its
`WalPosition`. When the committer returns a decided leader, the surrounding sub-DAG is written as
`WAL_ENTRY_COMMIT`. All three happen on the same thread, in the same loop iteration, so ordering is
trivial.

**Durability.** A second dedicated OS thread — spawned with name `wal-syncer` by the `TokioCtx` —
calls `wal_syncer.sync()` once per second
([`crates/dag/src/context.rs`](../crates/dag/src/context.rs)). The `dag.fsync` parameter (off by
default) additionally forces a sync on every new own block before it goes out on the wire; setting
it to `true` trades a few ms of latency for the guarantee that a proposer never publishes a block it
hasn't itself durably committed.

**Recovery.** On startup, `Storage::open` replays the entire WAL into a `RecoveredState` (last own
block, pending payloads with their `WalPosition`s, unprocessed incoming blocks, last committed
leader, the set of already-committed block references). `Core::open` then initialises from that
state and the node is effectively back where it crashed — no separate recovery protocol, no
state-transfer RPC, just replay.

## Cryptography

Two libraries cover everything: [`blake2`](https://crates.io/crates/blake2) for Blake2b content
hashing and [`ed25519-consensus`](https://crates.io/crates/ed25519-consensus) for Ed25519 signatures
on those hashes. Both are pulled in as direct dependencies of the `dag` crate.

**Signing** happens on the `dag` thread at block proposal time: `Block::new` asks
`CryptoEngine::sign` for a signature over the canonical block digest. **Verification** happens on
the tokio side on every ingested block, inside `NetworkSyncer::connection_task` before the block is
forwarded to the core. Verifying on the networking task avoids blocking the core on work that has
natural parallelism across peers, and a failed signature check drops the block without costing the
core any cycles.

A replica's Ed25519 keypair is loaded from its private config; the committee file holds only public
keys.

**Disabling crypto.** The `CryptoEngine` has an `enabled` flag
([`crates/dag/src/crypto.rs`](../crates/dag/src/crypto.rs)). When disabled, `sign` returns a
pre-computed dummy signature and digest, and `verify` accepts everything unconditionally — the
signature fields still travel on the wire so the block layout is unchanged. The flag is set by
`Replica::run` to `crypto_disabled || !protocol.require_crypto` at startup: the builder's
`with_crypto_disabled()` toggle forces it off (used by the simulator), and a future protocol can opt
out by setting `require_crypto: false` in its `Protocol` definition. Every protocol variant shipped
today requires real crypto.

## Protocol-Conditional Behaviour

The [`Protocol`](../crates/consensus/src/protocol.rs) struct captures the knobs a committer exposes
to the runtime. Two of them directly steer behaviour outside consensus:

### `require_crypto`

Consumed exactly once, at replica startup (see [Cryptography](#cryptography) above). There's no
runtime switch: either the replica runs with real signatures or it runs with the dummy engine for
the full lifetime of the process.

### `leader_wait`

The two regimes correspond to the two families of protocols:

- **Partially-synchronous / leader-waiting** — Mysticeti, Cordial Miners (partially synchronous).
  The committer's `get_leaders(round)` returns `Some(leaders)`. Before proposing at round `r+1`, the
  core checks that **every expected leader** at round `r` has already published its block. If not,
  the proposer waits up to the round timeout (default **1 s**) for the laggard before falling back
  to `ForceNewBlock`.

- **Asynchronous-friendly / quorum-waiting** — Cordial Miners (asynchronous), Mahi-Mahi.
  `get_leaders(round)` returns `None`. Before proposing, the core instead requires **every connected
  committee member** to have a block at the current quorum round — any block, no designated leader.
  The round timeout in this regime is much tighter (default **75 ms**): the protocol has no reason
  to wait on a particular proposer, only to smooth over scheduling noise.

The branch that implements this split is `Core::ready_new_block` in
[`crates/dag/src/core.rs`](../crates/dag/src/core.rs): the `Some(leaders)` vs. `None` return from
`get_leaders` picks which predicate is applied to the block-store check. The round-timeout constant
comes from `Protocol::default_round_timeout()` and can be overridden by the `dag.round_timeout`
parameter in the replica config.

## Out of Scope

A few things the codebase deliberately doesn't do, worth naming so readers don't expect them:

- **No application execution.** Transactions are opaque `Bytes`
  ([`crates/dag/src/block/transaction.rs`](../crates/dag/src/block/transaction.rs)); consensus never
  inspects them, there is no VM or state machine, and the block handler passes them through unread.
  Anything beyond "totally-ordered bytes" is the caller's problem. The one built-in producer is the
  benchmark **load generator** at
  [`crates/replica/src/generator.rs`](../crates/replica/src/generator.rs), which emits synthetic
  payloads with an embedded timestamp so the commit path can measure end-to-end latency.
- **No mempool sharding.** Unlike Narwhal, there are no worker processes and no separate mempool
  plane. The load generator talks directly to the block handler on the same replica.
- **No production peer discovery.** The committee is static: every replica loads the full public
  config at startup and opens connections to every other member. There is no gossip, no membership
  change protocol, no NAT traversal.
- **No erasure coding.** Blocks are replicated in full.
