# The Hydrozoan formalization

A Lean 4 + mathlib formalization of the safety and liveness results of the
Hydrozoan paper — the protocol implemented here as `DagHydrangea`
(`crates/consensus`) — and of its theory-only variant Optimal-Hydrozoan.
References to `sections/*.tex` in the docstrings name the paper's source
files (procedures of its Algorithm 2 and 3, and its lemma labels); the
paper is a separate repository. Every proof is machine-checked by the Lean kernel; the human
review effort is concentrated on an enumerable list of files — the
**audit surface** — that carries all the meaning. If those files say
what the paper means, the theorems hold; nothing outside them needs to
be read.

## The trust partition

| Layer          | Files                                                                                        | Status                                                                                                       |
| -------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Trusted core   | `Hydrozoan/Model/*.lean`                                                                     | definitions only, theorem-free; reviewed in full                                                             |
| Claims         | `Hydrozoan/<Result>/Statement.lean`                                                          | definitions and prose ending in `def Statement : Prop`; never a proof; reviewed                              |
| Witnesses      | `HydrozoanTest/*.lean`                                                                       | concrete instantiations keeping the definitions satisfiable and non-trivial; the instantiations are reviewed |
| Infrastructure | `lakefile.toml`, `lean-toolchain`, `scripts/check_no_holes.py`, `.github/workflows/lean.yml` | reviewed                                                                                                     |
| Generated      | `Hydrozoan/Helpers/*.lean`, `Hydrozoan/<Result>/Proof.lean`                                  | proofs; kernel-checked, never reviewed                                                                       |

The trusted core (`Model/`, ten files) defines the fault model and
thresholds (`Faults.lean`), blocks and validity (`Block.lean`), the
block universe and non-equivocation (`BlockUniverse.lean`), views and
causal reachability (`View.lean`, `CausalHistory.lean`), the slot
schedule (`Slots.lean`), the direct and graded-indirect decision rules
(`DirectRules.lean`, `IndirectRules.lean`, `Decided.lean`), and the
liveness hypotheses (`Liveness.lean`). Every declaration carries a
docstring phrased in the paper's terms, naming the corresponding
procedure in `sections/algorithms.tex` where one exists — the
docstrings are the audit interface.

## The results

Each result is one `Statement.lean` (what is claimed) plus a generated
`Proof.lean` proving `theorem holds : Statement`.

| Result                | Claim                                                                                                                                                                                                                                          |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ThresholdArithmetic` | the design's slack-cap table holds for every fault configuration and all `k ≥ 0` — no cap on `k` is needed                                                                                                                                     |
| `DirectSafety`        | per-slot exclusions among the direct rules: two fast commits agree, certificates are unique, two slow commits agree, fast and slow agree, commit excludes skip                                                                                 |
| `SlotAgreement`       | `DecidedUnique` — no two views ever decide one slot differently, across all six decision routes                                                                                                                                                |
| `PrefixAgreement`     | the safety headline: any two correct replicas' output ledgers are prefix-consistent, for every linearizer                                                                                                                                      |
| `DirectLiveness`      | a synchronised, populated wave with a correct leader slow-commits (plus the opportunistic fast/skip latency pair, deliberately outside `Statement`)                                                                                            |
| `IndirectLiveness`    | the graded indirect rule is total below an anchor, and a committed run decides every slot beneath it                                                                                                                                           |
| `EventualDecision`    | the liveness headline: under a fair schedule, verdict-covered prefixes grow past every slot                                                                                                                                                    |
| `Grounding`           | the liveness hypotheses are dischargeable: a premise-free fair schedule exists, the hypothesis package is realizable at every horizon by a `T`-only universe, and the composed conclusion is achievable with no premise beyond the fault model |

The witness files are load-bearing, not examples: they prove the
definitions satisfiable (finite block tables checked by `decide`),
exercise every decision route end-to-end, pin boundary cases at exact
quorums, and apply each `holds` theorem concretely so that a silently
strengthened hypothesis fails the build. `HydrozoanTest/Axioms.lean`
is the tripwire: it pins every headline theorem's axioms to exactly
`propext`, `Classical.choice`, `Quot.sound`, and fails the build on
any deviation — a smuggled axiom or a `sorry` anywhere in the
dependency tree cannot land silently.

## The Optimal-Hydrozoan arc (in progress)

The paper's Optimal-Hydrozoan (`sections/optimal-protocol.tex`,
`optimal-proof.tex`, `optimal-algorithms.tex`) is formalized as a
**parallel arc** under `Hydrozoan/Optimal/`, namespace
`Hydrozoan.Optimal`. It imports the frozen core above and never edits
it: the Optimal universe _extends_ `BlockUniverse` with FinWhale's
leader-exclusion clause, and every Hydrozoan lemma keeps applying to
the underlying universe. The arc adds its own trusted core
(`Hydrozoan/Optimal/Model/*.lean`, definitions only), its own
`Statement.lean` files, and witnesses under `HydrozoanTest/Optimal/`;
the same trust partition and audit rules apply to them.

What changes relative to Hydrozoan, in the model's terms:

| Hydrozoan                                    | Optimal-Hydrozoan                                                                                                               |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `p = ⌊(c+k)/2⌋`                              | `pOpt = ⌊(c+k)/2⌋ + 1` (Hydrangea's lower bound), on `OptimalFaults extends Faults` with `1 ≤ f + c`                            |
| `q_weak = f + p + 1`                         | dropped; per-block `tPlain = n − 2f − c − pOpt` and `tEquiv = f + pOpt`                                                         |
| `BlockUniverse`                              | `OptUniverse extends BlockUniverse`: a decision-round block that witnesses its slot's equivocation has no parent by that leader |
| `SkippedLeader`: `q_fast` blames             | `q_cert` blames **and** `q_cert` decision-round blocks that are fast evidence for no candidate                                  |
| rung 2 `WeakLinked`: `q_weak` anchored votes | `EvidenceLinked`: `q_cert` anchored decision-round blocks, each fast evidence for the candidate                                 |
| `Decided` (six routes, tie-break on rung 2)  | `DecidedOpt` (six routes, no tie-break — rung 2 is provably unique)                                                             |

Planned results (each a `Statement.lean` + generated `Proof.lean`,
mirroring the table above): `ThresholdArithmetic`, `DirectSafety`,
`SlotAgreement`, `PrefixAgreement`, then `DirectLiveness` — where the
direct skip of a candidate-less slot becomes a guaranteed claim rather
than an opportunistic one — `IndirectLiveness`, `EventualDecision`,
`Grounding`. The axioms tripwire is extended with each Optimal `holds`.

## Checking it

```sh
cd lean
lake exe cache get   # prebuilt mathlib (once per toolchain pin)
lake build           # kernel-checks every proof and witness model
python3 scripts/check_no_holes.py
```

`lake build` includes the witness models and the axioms tripwire, so a
green build already certifies: no `sorry`/`admit`/`axiom`/
`native_decide`/`unsafe`/`partial` anywhere (also enforced by
`check_no_holes.py` in pre-commit and CI), every `Statement` proven,
and the axiom pin intact. CI (`.github/workflows/lean.yml`) runs the
same checks on every push touching `lean/`.

## Provenance and fidelity

The development follows two published templates: the structural
modeling style of [`gdanezis/lean-dag`](https://github.com/gdanezis/lean-dag)
(a block-universe model with `Finset` counting, no operational
semantics) and the statement/proof trust partition of
[`joachimneu/auto-impossibility-experiment`](https://github.com/joachimneu/auto-impossibility-experiment).
Known fidelity gaps are documented on the definitions they concern
(see the docstrings in `Model/Block.lean` and `Model/Liveness.lean`):
references reach only the immediately preceding round (no weak links),
the DFS-vs-direct-reference vote equivalence is argued in prose, and
the synchrony hypothesis is assumed rather than derived from delivery
primitives.
