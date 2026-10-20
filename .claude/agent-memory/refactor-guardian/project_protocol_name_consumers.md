---
name: protocol-name-consumers
description: ConsensusProtocol serde tag and Debug name are external interfaces — consumed by YAML configs and by paper-repo parsers via measurement filenames
metadata:
  type: project
---

`ConsensusProtocol`'s Debug impl (e.g. `hydrozoan-l2-f3-c4-k2`) flows through
`NodeParameters` Debug into orchestrator `measurements-{parameters:?}.yaml` filenames
(`crates/orchestrator/src/collector.rs` save()). The serde kebab tag is what campaign
configs write. Neither reaches Prometheus labels.

**Why:** the 2026-10-20 DagHydrangea -> Hydrozoan rename left
`../hydrozoan-paper/data/evaluation/{plot.py,mysticeti-scripts/*.py}` still matching
`dag-hydrangea` (filename regex + emitted `protocol:` configs); archived releases
eval-0669999 / eval-649ec4f use the old name.

**How to apply:** on any protocol rename, grep sibling `*-paper` repos too, not only this
worktree; renames break those only at runtime.
