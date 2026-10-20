---
name: 277-latency-model-seam
description: Issue #277 simulator geography — LatencyModel/LinkLatency seam; what makes the Uniform variant RNG-equivalent to the old raw gen_range, and the invariants commit 2's regional model must keep
metadata:
  type: project
---

Issue #277 replaces `SimulatedNetwork`'s single `Range<Duration>` with a per-directed-link
`LatencyModel` (`crates/simulator/src/latency.rs`), so a later commit can add an asymmetric
regional model. Commit 1 is meant to be a pure refactor.

Invariants that make the `Uniform` variant stream-equivalent to the old
`rng.gen_range(range.clone())`:
- `LinkLatency::sample` must call `gen_range` with the *identical* `Range<Duration>`; rand 0.8's
  `SampleRange for Range<T>` delegates to `UniformDuration`, and any change to the range bounds
  or to the number of `gen_range` calls shifts the whole scheduler RNG stream.
- `base` is `Duration::ZERO` for `Uniform`, and `Duration::ZERO + d == d` exactly.
- `std::ops::Range::is_empty()` is `!(start < end)`, which is *exactly* rand's
  `SampleRange::is_empty` — so the `is_empty` short-circuit intercepts precisely the inputs that
  used to panic with "cannot sample empty range", and nothing else.
- Exactly one production RNG consumer exists in the crate: the `latency_channel` task. Adding a
  second (e.g. jitter, packet loss) reorders every downstream draw.

Direction convention in `SimulatedNetwork::connect` (`crates/simulator/src/network.rs`): the
channel whose receiver goes into `a_connection` carries **b -> a** traffic, so it must be built
from `link(b, a)`; the second channel carries a -> b and uses `link(a, b)`. Symmetric models
hide a swap here.

Equivocation interaction: the shim reflects a twin back to `from` through `inbound`, which is the
*reverse* channel of the same link. Under an asymmetric model the self-reflection therefore
incurs the peer->self latency, and a node receives its own twin at a different time per link.

**Why:** commit 2 adds a regional/asymmetric model on this seam, where a direction swap or an
extra draw would be a silent latent bug rather than a compile error.

**How to apply:** when reviewing the regional model, check the direction mapping first, then
count `gen_range` calls per delivery, then check `base` overflow (`Duration + Duration` panics on
overflow; a large regional base plus a large extra is no longer provably safe). Back the reading
up with a same-seed output diff built with the biased-select patch — see
[[simulator-nondeterminism]].

## Commit 2 (geographic model) — audited facts

- `LatencyModel` is now the serde config type itself (one `latency:` key). `validate()` runs in
  `SimulationRunner::run` before the executor; `LatencyModel::link`'s `expect` is unreachable
  because `Geography::link(i, j)` depends only on `region_of(i) = regions[i % len]`, and
  `validate` exhausts every slot pair `0..len × 0..len` — this covers committee_size > regions.len()
  and duplicate region names.
- **serde_yaml 0.9 enum shapes (verified empirically against 0.9.34):** plain serde writes enum
  variants as a YAML `!tag`, and the `!tag` form is accepted by a *direct* struct deserialize but
  **rejected** by an untagged enum ("untagged and internally tagged enums do not support enum
  input"). `#[serde(with = "serde_yaml::with::singleton_map")]` writes `variant: value` instead
  and delegates unit variants unchanged (`fullMesh` stays a plain string); its deserializer
  accepts a string or a single-key map but **not** a `!tag`. When #277 landed, `SimulationMode`
  derived `#[serde(untagged)]`, so every enum field on `SimulationConfig` needed `singleton_map`
  for the exported run config (cli `exporter.rs`) to replay through `SimulationMode::load`. That
  constraint is gone: `SimulationMode` now has a hand-written `Deserialize`
  (`crates/simulator/src/config.rs`) that reads a `serde_yaml::Value` and dispatches straight to a
  config or a suite. `singleton_map` on `latency` / `topology` is now a format choice (configs and
  exports use the `variant: value` map form, which its deserializer requires), not a requirement
  for new enum fields; the comment above those fields in `config.rs` still states the old reason.
  `#[serde(default, with = ...)]` is sound: `with` is not consulted when the key is absent.
- Region placement is claimed to match the orchestrator's
  `settings.regions.iter().cycle()` in `select_instances`. That is exact only if every region's
  pool still has an instance on each lap — the loop *silently skips* an exhausted region and
  shifts every later committee index — and if `settings.regions` is listed in the same order as
  `latency.geographic.regions`.
- **Display contract: listed regions only.** `Geography::validate` checks RTT entries for the
  listed regions, and `Display` (`crates/simulator/src/latency/geography.rs`) matches it: min/max
  RTT over listed region pairs via `round_trip_ms`, and `regions.len()` without deduplication.
  Stale `rtt_ms` rows for unlisted regions never reach the summary; the test
  `display_covers_listed_regions_only` pins this. (An earlier revision scanned the whole `rtt_ms`
  map and deduplicated through a `BTreeSet`.)
