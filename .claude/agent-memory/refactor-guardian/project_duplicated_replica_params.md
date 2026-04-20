---
name: Duplicated ReplicaParameters (replica + simulator)
description: ReplicaParameters intentionally duplicated in replica/src/params.rs and simulator/src/params.rs to avoid a crate cycle; unified after the replica→lib+cli split.
type: project
---

`ReplicaParameters { dag: DagParameters, consensus: ConsensusProtocol }` lives
verbatim in two places:
- `crates/replica/src/config.rs` (alongside `PublicReplicaConfig` et al.)
- `crates/simulator/src/params.rs`

**Why:** `simulator` cannot depend on `replica` today because `replica` is a
binary-shaped crate (owns the CLI) and pulls in runtime concerns simulator
doesn't want. The planned split (replica → `replica_lib` + `replica_cli`)
will let simulator depend on the lib. Until then, the user explicitly
approved the mirror and the simulator copy carries a `NOTE:` header.

**How to apply:** When reviewing edits to either file, diff against the other
and flag drift. Current structural state is identical: `#[derive(Serialize,
Deserialize, Clone, Default)]` + two `#[serde(default)]` fields `dag` +
`consensus` + `impl ImportExport`. Field order, derives, serde attributes,
and `ImportExport` impl must stay identical.
