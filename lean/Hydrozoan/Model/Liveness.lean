import Hydrozoan.Model.View

/-!
# Liveness hypotheses

Trusted core: the structural rendering of "after GST". Safety assumed
nothing about the network; every liveness theorem is exactly as strong
as the hypotheses here, so this file is the liveness arc's audit center
of gravity.

The derivation of these hypotheses from delivery primitives (received
sets, timeouts, view convergence) is deliberately out of scope: they
are **assumed**, with the fidelity argument recorded on each
definition.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica]

/-- Every replica in `T` authors a block at round `r`.

`T`-relative rather than all-of-`Correct`, deliberately: liveness
counts to quorums, never to every correct replica, and demanding all of
`Correct` would void the theorems whenever a single correct replica
misses a single round — a GC pause, a restart. Nothing is said about
uniqueness (universe non-equivocation already gives it for
non-Byzantine authors) or about references.

Nothing here constrains `T`: the requirements `T ⊆ Correct` and
`q ≤ T.card` are explicit hypotheses of the consuming theorems (the
subset condition alone would admit `T = ∅`) — asserting this predicate
for a `T` containing a Byzantine replica is asserting Byzantine
behavior, which no theorem does. Reducible so witness models
can settle it by `decide`. -/
@[reducible]
def PopulatedOn (U : BlockUniverse Replica BlockId)
    (T : Finset Replica) (r : ℕ) : Prop :=
  ∀ v ∈ T, ∃ b ∈ U.ids, (U.block b).round = r ∧ (U.block b).author = v

/-- The all-of-`Correct` case. -/
abbrev Populated (U : BlockUniverse Replica BlockId) (r : ℕ) : Prop :=
  PopulatedOn U (Correct : Finset Replica) r

/-- From round `R` on, every `T`-authored block references every
`T`-authored block of the round below. Precisely: the constrained
blocks are those at rounds `≥ R + 1` — a round-`R` block owes nothing
to round `R − 1`.

**An assumption, not a theorem.** A block's references are frozen when
it is built: a replica that builds on the first quorum it holds can
miss a slow correct block forever, even under perfect view convergence.
What makes this true of the deployed system in good periods is the
protocol's waiting rule — a correct replica builds a full timeout after
entering a round, never as soon as a quorum arrives — together with
timely post-stabilization delivery. Deriving it from those primitives
is future work; here it is assumed.

**`R` is not GST.** It is a round index — stabilization plus however
long catch-up ran. No clock and no `Δ` appear anywhere in the model.

`T` is a parameter, not a defined notion: it is instantiated as a
quorum of correct replicas participating steadily through the window —
authoring every round from `R` on and receiving peers' blocks in time —
and those properties are exactly what the hypotheses about `T` assert.

**Both quantifiers are `T`-restricted, deliberately.** A Byzantine
replica may publish nothing, or reveal blocks to only some replicas, so
assuming its blocks get referenced would assume Byzantine replicas
behave; and no crashed replica is mentioned — the hybrid model's
`Correct` pool is exactly the population liveness may lean on.

Compatibility with validity: when round `n` is `T`-populated, a block
referencing all of a quorum-sized `T`'s round-`n` blocks carries ≥ `q`
distinct authors, so `ValidWrt.quorum` is satisfiable alongside — the
witness models prove it.

**Known limitation — round-jumping recovery is not modeled.** `T` is
fixed across the whole suffix from `R`, so a correct replica that
recovers by jumping to the frontier round (authoring nothing for the
rounds it skipped) must sit outside `T` permanently, even after it has
rejoined the steady quorum. A finer, wave-scoped form (a per-round-pair
`SynchronisedAt` with a per-wave `T`) would readmit such a replica for
every wave it actually participates in; deliberately deferred. -/
def SynchronisedOn (U : BlockUniverse Replica BlockId)
    (T : Finset Replica) (R : ℕ) : Prop :=
  ∀ n, R ≤ n →                       -- at every round n from R on:
  ∀ b ∈ U.ids,                       -- every existing block b ...
    (U.block b).round = n + 1 →      -- ... sitting one round above n ...
    (U.block b).author ∈ T →         -- ... authored by a member of T,
  ∀ a ∈ U.ids,                       -- and every existing block a ...
    (U.block a).round = n →          -- ... sitting at round n ...
    (U.block a).author ∈ T →         -- ... also authored by a member of T:
    a ∈ (U.block b).parents          -- a is among b's parents

/-- The all-of-`Correct` case. -/
abbrev Synchronised (U : BlockUniverse Replica BlockId) (R : ℕ) : Prop :=
  SynchronisedOn U (Correct : Finset Replica) R

/-- Every correct replica's *eventual* view: the whole universe,
packaged as a `View`. The structural rendering of "eventually every
correct replica holds everything" — decision monotonicity transports
any view's verdicts into it, and it discharges every `CoversUpto`
hypothesis. Adds no information beyond `U` itself. -/
def View.full (U : BlockUniverse Replica BlockId) : View U :=
  ⟨U.ids, Finset.Subset.rfl, U.complete⟩

/-- **A view caught up to round `N`**: it holds every block of the
universe at a round at or below `N`. What a replica that has received
everything up to `N` holds — and the hypothesis under which a liveness
result holds of a replica's own view rather than of the eventual view.
The eventual view satisfies it at every `N`
(`View.coversUpto_full`, `Helpers/DirectLiveness.lean`). -/
def View.CoversUpto {U : BlockUniverse Replica BlockId}
    (V : View U) (N : ℕ) : Prop :=
  ∀ b ∈ U.ids, (U.block b).round ≤ N → b ∈ V.ids

end Hydrozoan
