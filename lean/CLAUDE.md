# Lean formalization — working rules

This directory holds the Lean 4 formalization of \sysname's safety and
liveness proofs. The rules below implement the project's division of labor:
**Claude authors everything; the author audits only definitions and theorem
statements.** Proofs are never audited — the kernel checks them.

## Layout: the trust partition

The directory structure separates what the author reviews from what is
generated, at file granularity (pattern: `joachimneu/auto-impossibility-experiment`):

- `Hydrozoan/Model/` — the **trusted core**: definitions only,
  theorem-free. Reviewed in full.
- `Hydrozoan/Helpers/` — generated lemma infrastructure. Unaudited.
- `Hydrozoan/<Result>/Statement.lean` — reviewed claim files: import
  `Model/` only; contain definitions and prose, culminating in
  `def Statement : Prop`; **never a proof** (enforced by
  `check_no_holes.py`). Auxiliary predicates a claim needs but the core
  should not carry are defined here, on the reviewed side — never in the
  proof layer.
- `Hydrozoan/<Result>/Proof.lean` + `Proof/` — generated:
  `theorem holds : Statement` and its lemma files. Never read by the author.
- `HydrozoanTest/` — witness models; the *instantiations* are reviewed.

The audit surface is exactly: everything under `Model/`, every
`Statement.lean`, the witness instantiations, plus `lakefile.toml`,
`lean-toolchain`, `scripts/check_no_holes.py`, and
`.github/workflows/lean.yml`. Everything else is generated and unaudited.

## The trusted core is read-only

**Do not edit, add to, rename, or delete anything under `Model/`, any
`Statement.lean`, or the infrastructure files above unless the current
request explicitly instructs such a change.** This holds even when a proof
appears to need it, even when the change looks trivial or obviously
correct. If in doubt: do not touch them.

Why — silent vacuity. A core change can hollow out the theorems with every
check still green: `lake build` passes, the hole checker passes, yet a
strengthened hypothesis has become unsatisfiable or a weakened predicate
trivially true, and the headline theorems now say nothing. For this
achievability-style development the failure modes are: a class or
structure hypothesis that no instance can satisfy (the witness models are
the mechanical guard — keep them biting), and a commit/decision predicate
weakened until conflicting outcomes are no longer excluded. None of these
trip a build error.

When a proof seems to need a core change, express what it needs as a
derived definition or lemma in `Helpers/` or the result's own
`Statement.lean` instead. If the model itself looks wrong — not merely
inconvenient — stop and raise it with the author; do not fix it unasked.

When a core change IS authorized: keep `Model/` definitions-only, extend
the witness models in the same change, sanity-check that no downstream
theorem became easier (a theorem that suddenly got easier is a red flag),
and report the core edit explicitly in the reply — never buried among
proof edits.

## Phase workflow (the working style)

Every phase runs the same loop; do not shortcut it, even when the scope
looks settled from a handoff doc or an earlier session.

1. **Discuss → agree.** Present the phase's scope and the design choices
   worth debating; stop and wait. Phases that introduce definitions
   (new `Model/` files, new `Statement.lean` shapes) get an explicit
   plan; phases that only re-state a known shape or only prove go
   straight to execution once agreed.
2. **Write the human-readable files first.** The audit surface —
   `Model/` definitions, `Statement.lean`, witness instantiations — is
   authored before any proof, in its own files (the trust partition
   above is the mechanism: human-readable and machine-checked content
   never share a file). Hand the file list to the author.
3. **Author review → "go" → freeze.** The author reads those files; on
   his explicit go they are frozen. No proof work starts before the go.
4. **Prove, with a verifier in parallel.** Build `Proof.lean` and the
   `Helpers/` lemmas. In parallel, run the `vacuity-auditor` agent
   (cold context: file paths only, no task background) over the frozen
   audit files and the witness files. It checks that no definition or
   claim is vacuous, that the witnesses are load-bearing — no useless
   test, no boundary or negative case missing for the phase's
   definitions — and that every new decision route or threshold is
   exercised. Relay its findings; fixes to reviewed files need the
   author's go again.
5. **Green → commit → next phase.** `lake build` + `check_no_holes.py`
   green, auditor findings resolved, then commit (on the author's go)
   and return to step 1.

## Pre-commit vacuity audit

Before committing any phase, run the `vacuity-auditor` agent
(`.claude/agents/vacuity-auditor.md`) on the phase's new or changed
audit-surface files — `Model/` definitions, `Statement.lean` files, and
witness files — passing **only the file paths**, no task background (the
cold context is the point). Relay its findings to the author before the
commit; apply agreed fixes first. Reviewed files touched by fixes need
the author's explicit go-ahead.

## Hard rules

- **Never introduce** `sorry`, `admit`, `axiom`, `native_decide`, `unsafe`,
  or `partial`. Enforced by `scripts/check_no_holes.py` (pre-commit + CI).
  If a proof won't close, leave the theorem out and say so — do not stub it.
- **Headline theorems** must depend on at most `propext`, `Classical.choice`,
  `Quot.sound` (`#print axioms`).
- **Witness models are load-bearing**, not examples: they prove the
  definitions satisfiable (and, via negative examples, non-trivial). They are
  default build targets; when a definition changes, extend them in the same
  change so a vacuous definition fails the build.
- **Docstrings are the audit interface.** Every trusted-base declaration
  carries one, phrased in the paper's terms and naming the corresponding
  procedure/threshold in `sections/algorithms.tex` where one exists. Keep
  them faithful; a stale docstring is an audit bug.
- **Toolchain and mathlib pins** (`lean-toolchain`, `lakefile.toml`) are
  bumped only as a deliberate, stated maintenance step — never as a side
  effect.

## Conventions

- Terminology follows the repo root `CLAUDE.md`: faulty parties are
  "Byzantine" or "crashed", non-faulty ones "correct" — never informal
  synonyms. Definitions mirror the paper's names (`qFast` for
  $q_{\mathit{fast}}$, etc.).
- Modeling style follows `gdanezis/lean-dag`: a structural block universe
  (no operational semantics), counting arguments as `Finset` cardinalities
  discharged by `omega`.
- Process follows `joachimneu/auto-impossibility-experiment`: statements
  separated from proofs so the audit surface stays an enumerable file list.
- **Instance diamonds in witness files.** Several witness files declare
  global `Faults (Fin n)` instances for the same `n` with different
  parameters (`HydrozoanTest/DirectLiveness.lean`'s `fourReplicas`,
  `IndirectLiveness.lean`'s `fiveReplicas`, `LivenessHardening.lean`'s
  `sixReplicas`). A file importing two of them resolves `qCert (Fin n)`
  against whichever wins priority and a table can silently pin the wrong
  configuration. Rule: a witness file never imports two, and whenever two
  instances of one type are in scope, tables name the instance
  explicitly (`@qCert (Fin 4) _ _ inst`).

## Phase plan (safety)

Each phase ends with a green build and an audit of the new trusted-base
lines; once audited, definitions are **frozen** — reopening one requires
flagging it again.

1. Fault model + thresholds (trusted base).
2. Threshold arithmetic — machine-checks the design note's slack-cap table
   for all `k ≥ 0`.
3. Structural DAG model: `BlockUniverse`, `View`, causal reachability.
4. Decision rules as definitions, mirroring `sections/algorithms.tex`
   procedure by procedure.
5. Slot safety: direct-rule theorems, plus the "fast commit with no
   certificate" witness (fast ⇏ eventually slow).
6. The seam: the graded indirect rule agrees with both direct paths
   (the two-case consistency argument).
7. Cross-view agreement → prefix agreement: the headline safety theorem,
   plus the `#print axioms` check in CI.

Liveness follows as a later phase group with its own hypothesis audit.

The Optimal-Hydrozoan arc lives in `gdanezis/lean-dag`
(`LeanDag/OptimalHydrozoan/`), a peer arc of this development's mirror
there; it is not developed here.

## Modeling decisions (settled 2026-08-14)

Following `gdanezis/lean-dag`; do not re-litigate silently.

- **Votes are direct references — no DFS.** A round-`(r+1)` block votes for
  a round-`r` block `L` iff `L ∈ refs`. One-vote-per-author-per-slot comes
  from block validity (`distinct_creators`: a valid block never references
  two blocks by the same author) plus universe-level non-equivocation for
  non-Byzantine authors. Faithful at wave length 3: leader copies can only
  appear among a voter's direct refs.
- **Two documented fidelity gaps**, to be stated in the phase-3 docstrings:
  (1) refs point only to the immediately preceding round (no weak links);
  (2) the DFS ≡ direct-ref equivalence is argued in prose, not in Lean
  (an optional later phase if ever needed).
- **No `leadersPerRound` constant.** An abstract `Slots`-style schedule
  (`slotRound` monotone unbounded + `leader`, keyed injectively); multiple
  leaders per round = slots sharing a round.
- **Pipelining is an instantiation, not a proof obligation.** Safety
  quantifies over schedules; the pipelined schedule appears only as a
  witness model.

## Build

```sh
lake exe cache get   # prebuilt mathlib (once per pin bump)
lake build           # kernel-checks every proof + witness models
python3 scripts/check_no_holes.py
```
