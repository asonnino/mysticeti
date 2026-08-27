import Hydrozoan.Model.IndirectRules

/-!
# The decision relation

Trusted core: the per-slot decision procedure of
`sections/algorithms.tex` (`TryDirectDecide` + `TryIndirectDecide` +
`DecideFromAnchor`), as an inductive relation. `Decided U V k (some L)`
says the replica holding view `V` may commit `L` at slot `k`;
`Decided U V k none` says it may skip the slot; *undecided* is the
absence of any derivation.

The relation is **order-free between constructors**: the paper checks
skip before commit and fast before slow operationally, but any
justifiable verdict is derivable here, and the safety theorems prove the
routes never disagree. Within the indirect rule, however, the paper's
strict grading `certificate → weak-quorum → skip` **is** encoded: the
weak rung fires only when no candidate has an anchor-linked certificate,
and the indirect skip only when both rungs are empty for every
candidate.

The anchor premises follow the paper's `TryIndirectDecide`: the anchor
is the **nearest eligible committed** slot — `Decided … j (some A)` with
every eligible slot strictly between decided `none` (skipped slots
cannot anchor; a committed one would be the nearer anchor). "Stop at the
first undecided slot" needs no encoding: an undecided in-between slot
simply leaves no derivation.

The weak rung commits the **least** qualifying candidate
(`[LinearOrder BlockId]`) — the paper's deterministic
`argmin digest` tie-break, since equivocating copies may tie at
`q_weak`. Rung 1 needs no tie-break: certificates are unique per slot
(`2·q_cert > n + f`, proved in the safety phase).
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- The verdicts a replica holding view `V` may reach on slot `k`. -/
inductive Decided (U : BlockUniverse Replica BlockId) (V : View U) :
    ℕ → Option BlockId → Prop
  /-- The fast path commits a candidate: `q_fast` votes in view. -/
  | directFast {k : ℕ} {L : BlockId} :
      IsLeaderBlock U k L → FastCommitInView U V L (S.slotRound k) →
      Decided U V k (some L)
  /-- The slow path commits a candidate: `q_slow` certificates in view. -/
  | directSlow {k : ℕ} {L : BlockId} :
      IsLeaderBlock U k L → SlowCommitInView U V L (S.slotRound k) →
      Decided U V k (some L)
  /-- The direct skip: `q_fast` blames in view (covers the case of no
  candidate at all — blames target the slot). -/
  | directSkip {k : ℕ} :
      SkippedLeaderInView U V k → Decided U V k none
  /-- Rung 1: anchored on the nearest eligible committed slot, a
  certificate for `L` is in the anchor's reach. -/
  | indirectCert {k j : ℕ} {A L : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      Decided U V j (some A) →
      -- j is the NEAREST such slot: every eligible slot in between skipped
      -- (an undecided one in between leaves this underivable — the paper's
      -- "stop at the first undecided slot")
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → Decided U V i none) →
      -- L is a candidate for slot k
      IsLeaderBlock U k L →
      -- rung 1: anchor-linked certificate
      CertifiedIn U A L (S.slotRound k) →
      Decided U V k (some L)
  /-- Rung 2: no candidate has an anchor-linked certificate, `L` clears
  the weak quorum, and `L` is the least candidate doing so (the
  deterministic tie-break — equivocating copies may tie). -/
  | indirectWeak {k j : ℕ} {A L : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      Decided U V j (some A) →
      -- j is the nearest such slot (as in indirectCert)
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → Decided U V i none) →
      -- rung 1 is empty for EVERY candidate — the strict grading:
      -- the weak rung may only fire when no certificate is in reach
      (∀ L', IsLeaderBlock U k L' → ¬ CertifiedIn U A L' (S.slotRound k)) →
      -- L is a candidate for slot k
      IsLeaderBlock U k L →
      -- rung 2: q_weak anchor-linked votes
      WeakLinked U A L (S.slotRound k) →
      -- deterministic tie-break: L is the least candidate clearing the rung
      (∀ L', IsLeaderBlock U k L' → WeakLinked U A L' (S.slotRound k) →
        ¬ L' < L) →
      Decided U V k (some L)
  /-- Rung 3: anchored, and both rungs are empty for every candidate. -/
  | indirectSkip {k j : ℕ} {A : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      Decided U V j (some A) →
      -- j is the nearest such slot (as in indirectCert)
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → Decided U V i none) →
      -- rung 1 empty for every candidate ...
      (∀ L, IsLeaderBlock U k L → ¬ CertifiedIn U A L (S.slotRound k)) →
      -- ... and rung 2 empty for every candidate: only then skip
      (∀ L, IsLeaderBlock U k L → ¬ WeakLinked U A L (S.slotRound k)) →
      Decided U V k none

end Hydrozoan
