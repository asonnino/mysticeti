import Hydrozoan.Model.Decided
import Hydrozoan.Model.Liveness
import Mathlib.Data.Finset.Max

/-!
# Helpers: indirect liveness

Generated: the totality of the graded rule (some rung always fires under
a nearest eligible committed anchor), the descent below a committed run
(fuel induction over the distance to the run, extracting each slot's
nearest eligible committed anchor with `Nat.find`), and decision
monotonicity across views (`decided_mono` / `decided_full` — the lemmas
`View.full`'s docstring promises).
-/

namespace Hydrozoan

section Eligibility

variable {Replica : Type*} [S : Slots Replica]

/-- An eligible anchor lies at a strictly later slot: if `j ≤ k` then
monotonicity puts `slotRound j` at or below `slotRound k`, inside `k`'s
decision window. -/
theorem lt_of_eligibleAsAnchor {k j : ℕ}
    (h : EligibleAsAnchor Replica k j) : k < j := by
  by_contra hle
  push Not at hle
  have hmono := S.mono hle
  simp only [EligibleAsAnchor, decisionRound] at h
  omega

end Eligibility

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica] {U : BlockUniverse Replica BlockId}

section Totality

variable [LinearOrder BlockId] [S : Slots Replica]

omit [DecidableEq BlockId] in
/-- Among the candidates clearing the weak rung there is a least one —
the deterministic tie-break `indirectWeak` demands. The candidates live
inside the finite `U.ids` (a candidate is a universe block), so
`Finset.min'` extracts the minimum. -/
theorem exists_least_weak_candidate {k r : ℕ} {A : BlockId}
    (h : ∃ L, IsLeaderBlock U k L ∧ WeakLinked U A L r) :
    ∃ L₀, IsLeaderBlock U k L₀ ∧ WeakLinked U A L₀ r ∧
      ∀ L', IsLeaderBlock U k L' → WeakLinked U A L' r → ¬ L' < L₀ := by
  classical
  obtain ⟨L, hL, hw⟩ := h
  set cand : Finset BlockId :=
    U.ids.filter fun L' => IsLeaderBlock U k L' ∧ WeakLinked U A L' r
  have hne : cand.Nonempty := ⟨L, Finset.mem_filter.mpr ⟨hL.1, hL, hw⟩⟩
  obtain ⟨hmem, hLB, hWL⟩ :
      cand.min' hne ∈ U.ids ∧ IsLeaderBlock U k (cand.min' hne) ∧
        WeakLinked U A (cand.min' hne) r := by
    have := Finset.mem_filter.mp (cand.min'_mem hne)
    exact ⟨this.1, this.2.1, this.2.2⟩
  refine ⟨cand.min' hne, hLB, hWL, fun L' hL' hw' => ?_⟩
  exact not_lt.mpr (cand.min'_le L' (Finset.mem_filter.mpr ⟨hL'.1, hL', hw'⟩))

/-- **The graded rule is total.** Under the shared anchor prefix of the
indirect constructors, some rung fires: a certificate hit
(`indirectCert`), else — rung 1 empty for every candidate — a weak hit
at the least clearing candidate (`indirectWeak`), else both rungs empty
and the slot skips (`indirectSkip`). Classical case analysis: the rung
tests are not decided, only split on. -/
theorem decided_of_anchor {V : View U} {k j : ℕ} {A : BlockId}
    (helig : EligibleAsAnchor Replica k j)
    (hj : Decided U V j (some A))
    (hmid : ∀ i, k < i → i < j → EligibleAsAnchor Replica k i →
      Decided U V i none) :
    ∃ v, Decided U V k v := by
  classical
  have hkj : k < j := lt_of_eligibleAsAnchor helig
  by_cases hc : ∃ L, IsLeaderBlock U k L ∧ CertifiedIn U A L (S.slotRound k)
  · obtain ⟨L, hL, hcert⟩ := hc
    exact ⟨some L, Decided.indirectCert hkj helig hj hmid hL hcert⟩
  · push Not at hc
    by_cases hw : ∃ L, IsLeaderBlock U k L ∧ WeakLinked U A L (S.slotRound k)
    · obtain ⟨L₀, hL₀, hw₀, hleast⟩ := exists_least_weak_candidate hw
      exact ⟨some L₀, Decided.indirectWeak hkj helig hj hmid hc hL₀ hw₀ hleast⟩
    · push Not at hw
      exact ⟨none, Decided.indirectSkip hkj helig hj hmid hc hw⟩

open Classical in
/-- **A committed run decides everything below it** (general endpoints:
slots `b … n` committed, `n` eligible for everything below `b`). Fuel
induction on the distance `b - i`: each slot below extracts its nearest
eligible committed anchor via `Nat.find`; an eligible slot under that
anchor is uncommitted by minimality, hence below `b` (it cannot sit in
the run), hence decided `none` by the induction hypothesis — exactly
the nearest-anchor premise, and totality closes the slot. -/
theorem decided_below_of_committed_run {V : View U} {b n : ℕ}
    (hbn : b ≤ n)
    (hspan : ∀ i, i < b → EligibleAsAnchor Replica i n)
    (hrun : ∀ j, b ≤ j → j ≤ n → ∃ B, Decided U V j (some B)) :
    ∀ i, i < b → ∃ v, Decided U V i v := by
  have key : ∀ d i, i < b → b - i ≤ d → ∃ v, Decided U V i v := by
    intro d
    induction d with
    | zero => intro i hi hd; omega
    | succ d ih =>
      intro i hi hd
      -- The nearest slot above `i` that is both eligible and committed.
      have hex : ∃ j, EligibleAsAnchor Replica i j ∧
          ∃ B, Decided U V j (some B) :=
        ⟨n, hspan i hi, hrun n hbn (le_refl n)⟩
      have hle : Nat.find hex ≤ n :=
        Nat.find_le ⟨hspan i hi, hrun n hbn (le_refl n)⟩
      obtain ⟨helig, B, hB⟩ := Nat.find_spec hex
      have hmid : ∀ i', i < i' → i' < Nat.find hex →
          EligibleAsAnchor Replica i i' → Decided U V i' none := by
        intro i' h1 h2 h3
        -- Minimality: an eligible slot below the anchor is uncommitted ...
        have hnc : ¬ ∃ C, Decided U V i' (some C) :=
          fun hcom => Nat.find_min hex h2 ⟨h3, hcom⟩
        -- ... so it cannot lie in the run, so it lies below `b`, so the
        -- induction hypothesis reaches it.
        have hi'b : i' < b := by
          by_contra he
          exact hnc (hrun i' (by omega) (by omega))
        obtain ⟨v, hv⟩ := ih i' hi'b (by omega)
        cases v with
        | none => exact hv
        | some C => exact absurd ⟨C, hv⟩ hnc
      exact decided_of_anchor helig hB hmid
  intro i hi
  exact key (b - i) i hi (le_refl _)

end Totality

section ViewMono

/-- A larger view holds every supporter the smaller one does. -/
theorem fastCommitInView_mono {V V' : View U} (hsub : V.ids ⊆ V'.ids)
    {L : BlockId} {r : ℕ} (h : FastCommitInView U V L r) :
    FastCommitInView U V' L r :=
  le_trans h (Finset.card_le_card (Finset.image_subset_image
    (Finset.inter_subset_inter Finset.Subset.rfl hsub)))

/-- A larger view holds every certificate the smaller one does. -/
theorem slowCommitInView_mono {V V' : View U} (hsub : V.ids ⊆ V'.ids)
    {L : BlockId} {r : ℕ} (h : SlowCommitInView U V L r) :
    SlowCommitInView U V' L r :=
  le_trans h (Finset.card_le_card (Finset.image_subset_image
    (Finset.inter_subset_inter Finset.Subset.rfl hsub)))

/-- A larger view holds every blame the smaller one does. -/
theorem skippedLeaderInView_mono [S : Slots Replica] {V V' : View U}
    (hsub : V.ids ⊆ V'.ids) {k : ℕ} (h : SkippedLeaderInView U V k) :
    SkippedLeaderInView U V' k :=
  le_trans h (Finset.card_le_card (Finset.image_subset_image
    (Finset.inter_subset_inter Finset.Subset.rfl hsub)))

/-- **Verdicts persist as a view grows.** Structural induction on the
derivation: the three direct rules are threshold counts over
view-intersected sets, monotone in the view; the three indirect rules
rebuild from the induction hypotheses, passing every rung premise —
positive and negative alike — across untouched. That transport is sound
precisely because the rung tests (`CertifiedIn`, `WeakLinked`) are
universe-level, not view-relative: were they view-relative, the negated
premises of `indirectWeak`/`indirectSkip` would be anti-monotone and
this lemma would be false. -/
theorem decided_mono [LinearOrder BlockId] [S : Slots Replica]
    {V V' : View U} (hsub : V.ids ⊆ V'.ids) {k : ℕ} {v : Option BlockId}
    (h : Decided U V k v) : Decided U V' k v := by
  induction h with
  | directFast hL hfc =>
      exact Decided.directFast hL (fastCommitInView_mono hsub hfc)
  | directSlow hL hsc =>
      exact Decided.directSlow hL (slowCommitInView_mono hsub hsc)
  | directSkip hsk =>
      exact Decided.directSkip (skippedLeaderInView_mono hsub hsk)
  | indirectCert hkj helig _ _ hL hcert ihj ihmid =>
      exact Decided.indirectCert hkj helig ihj ihmid hL hcert
  | indirectWeak hkj helig _ _ hnc hL hw htie ihj ihmid =>
      exact Decided.indirectWeak hkj helig ihj ihmid hnc hL hw htie
  | indirectSkip hkj helig _ _ hnc hnw ihj ihmid =>
      exact Decided.indirectSkip hkj helig ihj ihmid hnc hnw

/-- Any view's verdicts hold at the eventual view — the transport
`View.full`'s docstring promises. -/
theorem decided_full [LinearOrder BlockId] [S : Slots Replica]
    {V : View U} {k : ℕ} {v : Option BlockId}
    (h : Decided U V k v) : Decided U (View.full U) k v :=
  decided_mono V.subset_ids h

end ViewMono

end Hydrozoan
