import Hydrozoan.Model.Liveness
import Hydrozoan.Model.Decided

/-!
# Direct-commit liveness — statement

A synchronised, populated wave with a correct leader commits. One claim
forms `Statement`: `CommitLiveness`, concluding both the slow-commit
threshold and its harvest as a `Decided` verdict at the eventual view.
The opportunistic pair (`FastLatency`, `SkipLatency`) is
**deliberately outside `Statement`**: both are performance
characterizations — they fire exactly when the actual faults fit the
fast allowance `p` — not liveness.

The guaranteed path is the slow one, and that is the design decision
this phase verifies: with `c` crashes and `f` silent Byzantine
replicas, only `q = n − f − c` voters are certain, and `q < q_fast` in
general — so the fast path cannot be guaranteed, while
`q_cert ≤ q` (Phase 2's "slow path collectible" row) and `q_slow ≤ q`
make the slow path reachable by the guaranteed quorum alone.
-/

namespace Hydrozoan

namespace DirectLiveness

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- **Commit liveness**: a quorum-sized set of correct replicas,
populated through the wave's three rounds and synchronised from some
`R` at or before the wave, commits its correct leader — the slow-commit
threshold is met, and the decision logic outputs the commit verdict at
the eventual view (the harvest form the later phases consume).

`SlowCommit` here is a threshold fact, not a route: the fast path may
also fire in the same universe (the rule predicates are not
exclusive) — this is the one the guaranteed quorum always reaches. -/
def CommitLiveness (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (T : Finset Replica) (R k : ℕ),      -- for any set T, round R, slot k:
    T ⊆ (Correct : Finset Replica) →     -- T holds only correct replicas ...
    q Replica ≤ T.card →                 -- ... and is at least a DAG quorum,
    SynchronisedOn U T R →               -- T is internally synchronised from R,
    R ≤ S.slotRound k →                  -- the wave lies at or after R,
    PopulatedOn U T (S.slotRound k) →    -- T fills the propose round ...
    PopulatedOn U T (S.slotRound k + 1) →  -- ... the voting round ...
    PopulatedOn U T (S.slotRound k + 2) →  -- ... and the decision round,
    S.leader k ∈ T →                     -- and the slot's leader is in T:
    ∃ L, IsLeaderBlock U k L ∧           -- then a candidate exists,
      SlowCommit U L (S.slotRound k) ∧   -- the slow threshold is met,
      Decided U (View.full U) k (some L)  -- and its verdict is committed

/-- Direct-commit liveness over every fault configuration, schedule,
tie-break order, and block universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
    [Slots Replica] (U : BlockUniverse Replica BlockId),
    CommitLiveness U

/-- **Performance, not liveness — deliberately outside `Statement`.**
When the *actual* faults fit the fast allowance `p`, a synchronised,
populated wave with a correct leader fires the fast path in two rounds:
`|Correct| = n − |byzantine ∪ crashed| ≥ n − p = q_fast`. The
protocol's two-round latency claim; it needs all of `Correct` —
a quorum-sized `T` does not suffice in general (only when `f + c ≤ p`
does the quorum reach `q_fast`, as in the low-fault witness) — and only
the propose and voting rounds. -/
def FastLatency (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (R k : ℕ),                           -- for any round R and slot k:
    (F.byzantine ∪ F.crashed).card ≤ p Replica →  -- ACTUAL faults fit p,
    Synchronised U R →                   -- all correct synchronised from R,
    R ≤ S.slotRound k →                  -- the wave lies at or after R,
    Populated U (S.slotRound k) →        -- correct fill the propose round ...
    Populated U (S.slotRound k + 1) →    -- ... and the voting round,
    S.leader k ∈ (Correct : Finset Replica) →  -- and the leader is correct:
    ∃ L, IsLeaderBlock U k L ∧           -- then a candidate exists ...
      FastCommit U L (S.slotRound k)     -- ... and it fast-commits (2 rounds)

/-- **Performance, not liveness — the skip half of the opportunistic
pair.** With ≤ p actual faults, a slot whose leader produced no
candidate at all is skipped directly at the voting round: every
voting-round block blames it vacuously, and the correct pool alone
reaches the q_fast blame quorum. Beyond p faults the direct skip may
be unreachable — it is opportunistic, not guaranteed — and the slot
resolves indirectly instead (later phases). No synchrony hypothesis:
blames reference nothing. -/
def SkipLatency (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (k : ℕ),                             -- for any slot k:
    (F.byzantine ∪ F.crashed).card ≤ p Replica →  -- ACTUAL faults fit p,
    Populated U (S.slotRound k + 1) →    -- correct fill the voting round,
    (∀ L, ¬ IsLeaderBlock U k L) →       -- and no candidate exists:
    SkippedLeader U k                    -- then the slot skips directly

end DirectLiveness

end Hydrozoan
