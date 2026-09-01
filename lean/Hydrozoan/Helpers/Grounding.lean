import Mathlib.Data.Fintype.EquivFin
import Mathlib.Order.Interval.Finset.Nat
import Hydrozoan.Grounding.Statement
import Hydrozoan.Helpers.SlotAgreement
import Hydrozoan.Helpers.EventualDecision

/-!
# Helpers: grounding

Generated: the three conjuncts of the grounding statement — the
wave-aligned fairness arithmetic, the horizon-universe construction,
and the capstone composition.
-/

namespace Hydrozoan
namespace Grounding

section Fairness

/-- The correct pool is nonempty: it holds at least `q ≥ 1` members. -/
theorem correct_nonempty (Replica : Type*) [Fintype Replica]
    [DecidableEq Replica] [F : Faults Replica] :
    (Correct : Finset Replica).Nonempty :=
  Finset.card_pos.mp (lt_of_lt_of_le q_pos q_le_card_correct)

/-- Wave-aligned fairness: given a target slot `k`, place the run at
the correct replica `v`'s wave in the `k`-th rotation cycle — slot
`3 * (v + n * k)` opens a wave led by `v`, and it lies past `k`. -/
theorem waveRobinFair : WaveRobinFair := by
  intro n hn F k
  obtain ⟨v, hv⟩ := correct_nonempty (Fin n)
  refine ⟨3 * (v.val + n * k), ?_, ?_⟩
  · have hk : k ≤ n * k := Nat.le_mul_of_pos_left k hn
    omega
  · intro i hi
    have hleader : (waveRobin n hn).leader (3 * (v.val + n * k) + i) = v := by
      apply Fin.ext
      change (3 * (v.val + n * k) + i) / 3 % n = v.val
      rw [Nat.mul_add_div (by omega), Nat.div_eq_of_lt hi, Nat.add_zero,
        Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt v.isLt]
    rw [hleader]
    exact hv

end Fairness

section Encoding

variable {Replica : Type*}

/-- Division pinned by a two-sided bound: `r·m ≤ j < (r+1)·m` forces
`j / m = r`. -/
theorem div_eq_of_between {m r j : ℕ} (hm : 0 < m)
    (h1 : r * m ≤ j) (h2 : j < (r + 1) * m) : j / m = r := by
  have hle : r ≤ j / m := (Nat.le_div_iff_mul_le hm).mpr h1
  have hlt : j / m < r + 1 := Nat.div_lt_of_lt_mul (by rwa [Nat.mul_comm])
  omega

/-- Two numbers with equal quotients and equal residues are equal. -/
theorem eq_of_div_mod_eq {m i j : ℕ} (hdiv : i / m = j / m)
    (hmod : i % m = j % m) : i = j :=
  calc i = m * (i / m) + i % m := (Nat.div_add_mod i m).symm
    _ = m * (j / m) + j % m := by rw [hdiv, hmod]
    _ = j := Nat.div_add_mod j m

/-- The author of block `b` in the horizon universe: `T`'s member number
`b mod |T|` (under the canonical enumeration of `T`). -/
noncomputable def cyclicAuthor (T : Finset Replica) (hm : 0 < T.card)
    (b : ℕ) : Replica :=
  (T.equivFin.symm ⟨b % T.card, Nat.mod_lt b hm⟩ : {x // x ∈ T})

theorem cyclicAuthor_mem (T : Finset Replica) (hm : 0 < T.card) (b : ℕ) :
    cyclicAuthor T hm b ∈ T :=
  (T.equivFin.symm _).2

/-- Two blocks share an author exactly when their indices agree mod
`|T|`. -/
theorem cyclicAuthor_inj (T : Finset Replica) (hm : 0 < T.card) {a b : ℕ}
    (h : cyclicAuthor T hm a = cyclicAuthor T hm b) :
    a % T.card = b % T.card :=
  congrArg Fin.val (T.equivFin.symm.injective (Subtype.coe_injective h))

/-- Block `r * |T| + index(v)` is `v`'s round-`r` block. -/
theorem cyclicAuthor_index (T : Finset Replica) (hm : 0 < T.card)
    {v : Replica} (hv : v ∈ T) (r : ℕ) :
    cyclicAuthor T hm (r * T.card + (T.equivFin ⟨v, hv⟩).val) = v := by
  have hmod : (r * T.card + (T.equivFin ⟨v, hv⟩).val) % T.card
      = (T.equivFin ⟨v, hv⟩).val := by
    rw [Nat.mul_comm r T.card, Nat.mul_add_mod]
    exact Nat.mod_eq_of_lt (T.equivFin ⟨v, hv⟩).isLt
  have harg : (⟨(r * T.card + (T.equivFin ⟨v, hv⟩).val) % T.card,
      Nat.mod_lt _ hm⟩ : Fin T.card) = T.equivFin ⟨v, hv⟩ := Fin.ext hmod
  unfold cyclicAuthor
  rw [harg, Equiv.symm_apply_apply]

/-- Block `b` of the horizon universe: round `b / |T|`, author `b mod
|T|` (cyclically through `T`), referencing ALL of the previous round's
blocks. Genesis needs no special case: at round `0` the parent interval
`Ico ((0−1)·|T|) (0·|T|)` is empty by ℕ subtraction. -/
noncomputable def horizonBlock (T : Finset Replica) (hm : 0 < T.card)
    (b : ℕ) : Block Replica ℕ where
  round := b / T.card
  author := cyclicAuthor T hm b
  parents := Finset.Ico ((b / T.card - 1) * T.card) (b / T.card * T.card)

@[simp] theorem horizonBlock_round (T : Finset Replica) (hm : 0 < T.card)
    (b : ℕ) : (horizonBlock T hm b).round = b / T.card := rfl

@[simp] theorem horizonBlock_author (T : Finset Replica) (hm : 0 < T.card)
    (b : ℕ) : (horizonBlock T hm b).author = cyclicAuthor T hm b := rfl

@[simp] theorem horizonBlock_parents (T : Finset Replica) (hm : 0 < T.card)
    (b : ℕ) : (horizonBlock T hm b).parents
      = Finset.Ico ((b / T.card - 1) * T.card) (b / T.card * T.card) := rfl

/-- Parent-interval membership pins the parent's round to the one
below (and forces the child's round positive). -/
theorem horizonBlock_parent_round (T : Finset Replica) (hm : 0 < T.card)
    {b j : ℕ} (hj : j ∈ (horizonBlock T hm b).parents) :
    j / T.card + 1 = b / T.card := by
  rw [horizonBlock_parents, Finset.mem_Ico] at hj
  have hpos : 0 < b / T.card := by
    rcases Nat.eq_zero_or_pos (b / T.card) with hz | hz
    · rw [hz, Nat.zero_mul] at hj
      exact absurd hj.2 (Nat.not_lt_zero j)
    · exact hz
  obtain ⟨r, hr⟩ : ∃ r, b / T.card = r + 1 := ⟨b / T.card - 1, by omega⟩
  rw [hr] at hj ⊢
  rw [Nat.add_sub_cancel] at hj
  rw [div_eq_of_between hm hj.1 hj.2]

end Encoding

section Universe

variable {Replica : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica]

/-- The horizon universe for `T` and `N`: `(N+1) · |T|` blocks — for
each round `r ≤ N`, one block per member of `T` (block `r * |T| + i`
belongs to member `i`), every non-genesis block referencing ALL of the
previous round's blocks. -/
noncomputable def horizonUniverse (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) : BlockUniverse Replica ℕ where
  ids := Finset.range ((N + 1) * T.card)
  block := horizonBlock T hm
  complete := by
    intro b hb j hj
    rw [horizonBlock_parents, Finset.mem_Ico] at hj
    rw [Finset.mem_range] at hb ⊢
    calc j < b / T.card * T.card := hj.2
      _ ≤ b := Nat.div_mul_le_self b T.card
      _ < (N + 1) * T.card := hb
  valid := by
    intro b _
    refine ⟨fun j hj => horizonBlock_parent_round T hm hj, ?_, ?_⟩
    · -- distinct authors: same residue and same round-interval force
      -- equality
      intro i hi j hj hauth
      rw [horizonBlock_author, horizonBlock_author] at hauth
      have hmod := cyclicAuthor_inj T hm hauth
      have hi' := horizonBlock_parent_round T hm hi
      have hj' := horizonBlock_parent_round T hm hj
      exact eq_of_div_mod_eq (by omega) hmod
    · -- quorum: the parent interval carries |T| distinct authors
      intro hpos
      rw [horizonBlock_round] at hpos
      have hinj : Set.InjOn (fun i => (horizonBlock T hm i).author)
          ↑(horizonBlock T hm b).parents := by
        intro i hi j hj hauth
        rw [Finset.mem_coe] at hi hj
        simp only [horizonBlock_author] at hauth
        have hmod := cyclicAuthor_inj T hm hauth
        have hi' := horizonBlock_parent_round T hm hi
        have hj' := horizonBlock_parent_round T hm hj
        exact eq_of_div_mod_eq (by omega) hmod
      have hcard := Finset.card_image_of_injOn hinj
      unfold authors authorsOf
      rw [hcard, horizonBlock_parents, Nat.card_Ico]
      obtain ⟨r, hr⟩ : ∃ r, b / T.card = r + 1 := ⟨b / T.card - 1, by omega⟩
      rw [hr, Nat.add_sub_cancel, Nat.succ_mul]
      omega
  no_equivocation := by
    intro i _ j _ _ hauth hround
    rw [horizonBlock_author, horizonBlock_author] at hauth
    rw [horizonBlock_round, horizonBlock_round] at hround
    exact eq_of_div_mod_eq hround (cyclicAuthor_inj T hm hauth)

@[simp] theorem horizonUniverse_ids (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) :
    (horizonUniverse T hm hq N).ids = Finset.range ((N + 1) * T.card) := rfl

@[simp] theorem horizonUniverse_block (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) :
    (horizonUniverse T hm hq N).block = horizonBlock T hm := rfl

/-- Every block of the horizon universe is authored by a member of
`T`. -/
theorem horizonUniverse_authors (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) :
    ∀ b ∈ (horizonUniverse T hm hq N).ids,
      ((horizonUniverse T hm hq N).block b).author ∈ T :=
  fun b _ => cyclicAuthor_mem T hm b

/-- The horizon universe populates every round up to its horizon. -/
theorem horizonUniverse_populated (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N r : ℕ) (hr : r ≤ N) :
    PopulatedOn (horizonUniverse T hm hq N) T r := by
  intro v hv
  refine ⟨r * T.card + (T.equivFin ⟨v, hv⟩).val, ?_, ?_, ?_⟩
  · rw [horizonUniverse_ids, Finset.mem_range]
    have hlt := (T.equivFin ⟨v, hv⟩).isLt
    have hmul : (r + 1) * T.card ≤ (N + 1) * T.card :=
      Nat.mul_le_mul_right _ (by omega)
    have hstep : (r + 1) * T.card = r * T.card + T.card := Nat.succ_mul r _
    omega
  · rw [horizonUniverse_block, horizonBlock_round]
    have hlt := (T.equivFin ⟨v, hv⟩).isLt
    apply div_eq_of_between hm (Nat.le_add_right _ _)
    rw [Nat.succ_mul]
    omega
  · rw [horizonUniverse_block, horizonBlock_author]
    exact cyclicAuthor_index T hm hv r

/-- The horizon universe is internally synchronised from round `0`:
every block's parents are ALL of the previous round's blocks, `T`'s or
not (in this universe, all blocks are `T`'s anyway). -/
theorem horizonUniverse_synchronised (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) :
    SynchronisedOn (horizonUniverse T hm hq N) T 0 := by
  intro n _ b _ hbround _ a _ haround _
  rw [horizonUniverse_block, horizonBlock_round] at hbround haround
  rw [horizonUniverse_block, horizonBlock_parents, hbround,
    Nat.add_sub_cancel, Finset.mem_Ico]
  constructor
  · have := Nat.div_mul_le_self a T.card
    rwa [haround] at this
  · have had := Nat.div_add_mod a T.card
    have hma := Nat.mod_lt a hm
    rw [haround, Nat.mul_comm] at had
    rw [Nat.succ_mul]
    omega

/-- The realizability conjunct: the horizon universe is `T`-only and
discharges both hypotheses at once. -/
theorem hypothesesRealizable : HypothesesRealizable := by
  intro Replica _ _ _ T N hq
  have hm : 0 < T.card := lt_of_lt_of_le q_pos hq
  exact ⟨horizonUniverse T hm hq N,
    horizonUniverse_authors T hm hq N,
    fun r hr => horizonUniverse_populated T hm hq N r hr,
    horizonUniverse_synchronised T hm hq N⟩

end Universe

section Progress

/-- The capstone composition: fairness places a correct-led run past
`k`, the horizon universe realizes the hypotheses over the run's span
with `T = Correct`, direct liveness commits the run's first slot, and
the descent settles everything below it. -/
theorem groundedProgress : GroundedProgress := by
  intro n hn F k
  let S : Slots (Fin n) := waveRobin n hn
  obtain ⟨b, hkb, hlead⟩ := waveRobinFair n hn k
  have hq : q (Fin n) ≤ (Correct : Finset (Fin n)).card := q_le_card_correct
  have hm : 0 < (Correct : Finset (Fin n)).card := lt_of_lt_of_le q_pos hq
  set U : BlockUniverse (Fin n) ℕ :=
    horizonUniverse (Correct : Finset (Fin n)) hm hq (b + 4)
  have hpop : ∀ r, r ≤ b + 4 → PopulatedOn U (Correct : Finset (Fin n)) r :=
    fun r hr => horizonUniverse_populated _ hm hq _ r hr
  have hsync : SynchronisedOn U (Correct : Finset (Fin n)) 0 :=
    horizonUniverse_synchronised _ hm hq _
  have hspan : IndirectLiveness.SpansEligible (Fin n) 3 := by
    intro b' i hi
    change i + 2 < b' + 3 - 1
    omega
  have hleadb : S.leader b ∈ (Correct : Finset (Fin n)) := by
    have := hlead 0 (by omega)
    rwa [Nat.add_zero] at this
  refine ⟨b, hkb, U, ?_⟩
  intro V hcov
  obtain ⟨L, -, -, hL⟩ :=
    DirectLiveness.holds (Fin n) ℕ U (Correct : Finset (Fin n)) 0 b
      Finset.Subset.rfl hq hsync (Nat.zero_le _) (hpop b (by omega))
      (hpop (b + 1) (by omega)) (hpop (b + 2) (by omega)) hleadb
      V (hcov.mono (by change b + 2 ≤ b + 4; omega))
  have hbelow :=
    EventualDecision.runDecidesBelow U (Correct : Finset (Fin n)) 0 b 3
      Finset.Subset.rfl hq hsync (by omega) hspan (Nat.zero_le _) hlead
      (fun r _ h2 => hpop r h2) V (hcov.mono (by change b + 4 ≤ b + 4; omega))
  exact ⟨⟨L, hL⟩, hbelow⟩

end Progress

end Grounding
end Hydrozoan
