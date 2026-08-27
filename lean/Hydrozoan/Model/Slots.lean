import Hydrozoan.Model.BlockUniverse

/-!
# Slots and the leader schedule

Trusted core: the abstract slot schedule of the paper's "Waves and
pipelining" paragraph (`sections/algorithms.tex`). A **slot** is one
leader-decision instance: slot `k` is proposed at `slotRound k` by
`leader k`, voted on one round later, and (slow-)decided two rounds
later. There is no `leadersPerRound` constant: multiple leaders per
round are simply slots sharing a round, and the paper's ranked iteration
over `(round, leaderOffset)` pairs becomes enumeration by slot index.

Pipelining is an *instantiation*, not a proof obligation: safety
quantifies over all schedules, and the pipelined schedule (every round a
propose round) appears only as a witness instance.
-/

namespace Hydrozoan

/-- The slot schedule: which round each slot is proposed in and which
replica leads it. -/
class Slots (Replica : Type*) where
  /-- The round at which slot `k` is proposed (the paper's
  `ProposeRound`). -/
  slotRound : ℕ → ℕ
  /-- The replica whose block is the slot-`k` candidate. -/
  leader : ℕ → Replica
  /-- Slots are enumerated in round order. -/
  mono : Monotone slotRound
  /-- Slot rounds are unbounded. -/
  unbounded : ∀ n, ∃ k, n ≤ slotRound k
  /-- Distinct slots differ in round or in leader. -/
  keyed : Function.Injective fun k => (slotRound k, leader k)

section SlotArithmetic

variable (Replica : Type*) [S : Slots Replica]

/-- The round at which slot `k` is voted on (the paper's
`VotingRound`): votes for the slot's candidate live here. -/
def votingRound (k : ℕ) : ℕ := S.slotRound k + 1

/-- The round at which slot `k`'s slow path is settled (the paper's
`DecisionRound`): its certificates live here. -/
def decisionRound (k : ℕ) : ℕ := S.slotRound k + 2

end SlotArithmetic

section LeaderBlocks

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica] [S : Slots Replica]

/-- `L` is a candidate block for slot `k`: the right round, the right
author (the paper's `GetLeaderBlocks`, as a membership predicate).
Because replicas may equivocate, several blocks can satisfy this for one
slot — the rules count authors, and the graded rule's tie-break picks
among copies. Reducible so decidability is inferable inside filters. -/
@[reducible]
def IsLeaderBlock (U : BlockUniverse Replica BlockId) (k : ℕ) (L : BlockId) :
    Prop :=
  L ∈ U.ids ∧ (U.block L).round = S.slotRound k ∧ (U.block L).author = S.leader k

end LeaderBlocks

end Hydrozoan
