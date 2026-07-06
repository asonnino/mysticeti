# Mysticeti — uncertified DAG consensus

Reference implementations of several uncertified DAG-based consensus protocols
(Mysticeti, Mahi-Mahi, Blue Bottle, Cordial Miners PS/Async, Nemo-Nemo, DagHydrangea).
The codebase is
deliberately small and modular, meant for benchmarking and experimentation — **not
production** — but uses real networking, cryptography, and persistent storage. Ships with
a discrete-event simulator and a cloud orchestrator for running the protocols under custom
scenarios.

Rust **edition 2024**, toolchain pinned to **1.92** (see `rust-toolchain.toml`).

## Workspace layout

A Cargo workspace (`resolver = "3"`); `cargo run` from the root resolves to the `cli`
crate's `replica` binary.

| Crate | Responsibility |
| --- | --- |
| `crates/dag` | DAG primitives + storage the protocols run on: `Block`, `Authority`, `Committee`, `Core` (block manager + threshold clock), `Storage` (WAL-backed and ephemeral), `BlockReader`. |
| `crates/consensus` | The commit rule, decoupled from the DAG: `Committer` applies direct/indirect rules, `Wave` owns round/wave arithmetic, `protocol.rs` holds per-protocol parameter sets. |
| `crates/replica` | A running node: wires a `dag::Core` to a consensus `Committer`, networking, storage, metrics, and a load generator. Also hosts the `LocalTestbedRunner`. |
| `crates/orchestrator` | Cloud-testbed lifecycle (deploy/start/stop/destroy) over SSH + Prometheus metric collection. Protocol-agnostic; the CLI supplies the glue. |
| `crates/simulator` | Discrete-event simulator for exercising protocols under custom network/failure scenarios. |
| `crates/cli` | Command-line entrypoint (`local-testbed`, remote testbed/orchestrator commands). |
| `crates/minibytes` | Vendored zero-copy `Bytes` type (from Sapling SCM). Rarely touched. |

See `docs/architecture.md`, `docs/simulator.md`, and `docs/orchestrator.md` for deeper
design notes.

## Common commands

```sh
cargo run local-testbed              # boot a 4-replica local testbed + load gen (20s)
cargo build                          # build the workspace
cargo test -p consensus              # test one crate
cargo test                           # test the workspace
cargo clippy --workspace --all-targets
cargo fmt
```

A pre-commit hook chain runs on every commit: `cargo-fmt`, `clippy`, `cargo-test`,
license headers (licensesnip), `typos`, editorconfig, `taplo` (TOML), `shellcheck`,
`yamlfmt`. Commits fail if any of these fail — fix the underlying issue rather than
bypassing the hook.

## Consensus model (orientation)

All supported protocols share one committer; they differ only in a flat parameter set
(`Protocol`: `direct_commit_quorum`, `direct_skip_quorum`, `certificate_quorum`,
`quorum_threshold`, `fast_path`, `anchor_link_size`, `wave_length`, `leader_count`,
`pipeline`, `leader_wait`). `ConsensusProtocol` is the user-facing enum that validates
inputs and builds a `Protocol`. Rounds group into *waves* (leader round → voting
round(s) → decision round); a leader is **directly** committed/skipped from votes/blames
in its own wave, or **indirectly** decided later via an anchor. With `leader_count > 1`
a round elects a *cohort* of K leaders (offsets `0..K`). Dual-path protocols
(DagHydrangea) set `fast_path: Some(..)`: a fast direct commit from votes at the voting
round, reconciled with the certified slow path by a graded indirect rule
(certificate → weak quorum → skip).

## Conventions

- **Merging PRs: prefer rebase-merge over squash.** Rebase replays the branch's
  individual commits onto `main`, keeping each clean conventional-commit step in the
  history. Squash collapses a PR into one commit and loses that. Merge commits are
  disabled on this repo (`allow_merge_commit: false`), so rebase is the history-preserving
  option. When a branch falls behind `main`, rebase your local un-pushed work onto the
  updated tip; don't force-push rewritten shared history.
- **Commit messages** follow Conventional Commits (`feat(consensus):`, `fix(dag):`,
  `refactor(...)`, `test(...)`, …).
- **Test-only APIs** are gated behind `#[cfg(any(test, feature = "test-utils"))]`.
  Integration tests under `crates/<crate>/tests/` are separate crates and don't see
  `cfg(test)`, so they enable the feature (consensus does this via a self `path`
  dev-dependency). Shared DAG test builders live in `dag::test_util`
  (`build_dag`, `build_dag_layer`, `build_split_chain`, `drop_leader`, `committee`).
- **Style** (enforced by hooks): 100-char max line length; line-continuation indents are
  4-space multiples (not alignment padding). Use descriptive names, not C-style
  abbreviations. No section-divider comment banners — use modules/idioms.

## Gotchas

- **Checking if a branch is merged:** past PRs landed via **squash**, so a merged
  branch's commits live on `main` under a single new SHA. `git branch --merged` and
  `git cherry` (ancestry / patch-id) report such branches as *unmerged* — a false
  negative. Use `gh pr list --state merged --json headRefName,number` to decide whether a
  local branch is safe to delete.
- **`dag` features:** `tempfile` is a regular dependency (it backs `Storage::ephemeral`,
  used by the `local-testbed` command and the simulator — production code paths). The
  `test-utils` feature gates extra test helpers/constructors, not ephemeral storage.
- **Orchestrator settings format:** `cloud_provider` in the testbed `settings.yml` is a
  serde tagged enum, and serde_yaml serializes it as a **YAML tag** — write
  `cloud_provider: !aws` (or `!custom`) with the provider fields nested beneath, not a
  plain string or an `aws:`/`custom:` map. Per-provider starting points live in
  `crates/orchestrator/assets/settings-aws-template.yml` and `settings-custom-template.yml`;
  `check-yaml` runs with `--unsafe` so its safe loader doesn't choke on the custom tags.
