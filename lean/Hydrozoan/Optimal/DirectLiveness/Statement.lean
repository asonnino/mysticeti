import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.Model.Liveness

/-!
# Optimal-Hydrozoan: direct liveness — statement

Two claims form `Statement`. `CommitLiveness` is Hydrozoan's: a
synchronised, populated wave with a correct leader slow-commits — the slow
path is unchanged in Optimal-Hydrozoan, and so is the guaranteed commit;
only the harvest differs (`DecidedOpt`). `SkipLiveness` is what the arc
adds: a slot whose leader produced **no candidate** is directly skipped by
the guaranteed quorum alone — `q_cert ≤ q ≤ |T|` blames at the voting
round, and every decision-round block is fast evidence for nothing,
vacuously. No fault-count hypothesis and no synchrony hypothesis appear.
In Hydrozoan the same skip needs `q_fast = n − p` blames, which only
`Correct` can supply when the actual faults fit `p`; it is opportunistic
there (`SkipLatency`, kept outside `Statement`), and a liveness claim here
(decision D5). The price, recorded in the paper: the verdict lands at the
decision round, one round later than Hydrozoan's.

`SkipLiveness` is deliberately restricted to candidate-less slots. With a
candidate present, the Byzantine replicas can vote for it while every
correct replica blames it, and those `f` votes suffice to make every
correct decision-round block fast evidence whenever `f ≥ tPlain` — at the
minimal committee `tPlain = f + pOpt − 1` when `c + k` is even and
`f + pOpt` when odd, so the attack exists exactly at `c = k = 0` — and the
skip is not guaranteed: the paper's remark on FinWhale's attack. Such a
slot resolves indirectly.

`FastLatency` stays outside `Statement`, as in Hydrozoan: a performance
characterization, firing exactly when the actual faults fit the fast
allowance — now `pOpt`, one more than Hydrozoan's `p`.
-/

namespace Hydrozoan

namespace Optimal

namespace DirectLiveness

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- **Commit liveness** (Hydrozoan's, harvested as `DecidedOpt`): a
quorum-sized set of correct replicas, populated through the wave's three
rounds and synchronised from some `R` at or before the wave, commits its
correct leader — the slow-commit threshold is met, and the decision logic
outputs the commit verdict at the eventual view.

`SlowCommit` here is a threshold fact, not a route: the fast path may also
fire in the same universe — this is the one the guaranteed quorum always
reaches. -/
def CommitLiveness (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (T : Finset Replica) (R k : ℕ),      -- for any set T, round R, slot k:
    T ⊆ (Correct : Finset Replica) →     -- T holds only correct replicas ...
    q Replica ≤ T.card →                 -- ... and is at least a DAG quorum,
    SynchronisedOn U.toBlockUniverse T R →  -- T is internally synchronised from R,
    R ≤ S.slotRound k →                  -- the wave lies at or after R,
    PopulatedOn U.toBlockUniverse T (S.slotRound k) →      -- T fills the propose round ...
    PopulatedOn U.toBlockUniverse T (S.slotRound k + 1) →  -- ... the voting round ...
    PopulatedOn U.toBlockUniverse T (S.slotRound k + 2) →  -- ... and the decision round,
    S.leader k ∈ T →                     -- and the slot's leader is in T:
    ∃ L, IsLeaderBlock U.toBlockUniverse k L ∧           -- then a candidate exists,
      SlowCommit U.toBlockUniverse L (S.slotRound k) ∧   -- the slow threshold is met,
      DecidedOpt U (View.full U.toBlockUniverse) k (some L)  -- and its verdict is committed

/-- **Skip liveness** (the arc's addition): a slot with no candidate is
directly skipped by any quorum-sized set of correct replicas that fills
its voting and decision rounds — every voting-round block of `T` blames
the slot, every decision-round block of `T` is fast evidence for no
candidate, and `q_cert ≤ q ≤ |T|` — and the skip verdict is output at the
eventual view. No synchrony and no fault-count hypothesis: blames and
no-evidence reference nothing. `q ≤ |T|` is deliberately the DAG quorum,
uniform with `CommitLiveness`, although `q_cert ≤ |T|` would suffice. -/
def SkipLiveness (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (T : Finset Replica) (k : ℕ),        -- for any set T and slot k:
    T ⊆ (Correct : Finset Replica) →     -- T holds only correct replicas ...
    q Replica ≤ T.card →                 -- ... and is at least a DAG quorum,
    PopulatedOn U.toBlockUniverse T (S.slotRound k + 1) →  -- T fills the voting round ...
    PopulatedOn U.toBlockUniverse T (S.slotRound k + 2) →  -- ... and the decision round,
    (∀ L, ¬ IsLeaderBlock U.toBlockUniverse k L) →         -- and no candidate exists:
    SkippedLeaderOpt U.toBlockUniverse k ∧                 -- then the slot skips directly,
      DecidedOpt U (View.full U.toBlockUniverse) k none    -- and the verdict is output

/-- Direct liveness of Optimal-Hydrozoan, over every fault configuration,
schedule, and universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
    (U : OptUniverse Replica BlockId),
    CommitLiveness U ∧ SkipLiveness U

/-- **Performance, not liveness — deliberately outside `Statement`.**
When the *actual* faults fit the Optimal fast allowance `pOpt`, a
synchronised, populated wave with a correct leader fires the fast path in
two rounds: `|Correct| = n − |byzantine ∪ crashed| ≥ n − pOpt = q_fast`.
One more actual fault than Hydrozoan's `FastLatency` admits. It needs all
of `Correct` — a quorum-sized `T` does not suffice in general — and only
the propose and voting rounds. -/
def FastLatency (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (R k : ℕ),                           -- for any round R and slot k:
    (O.byzantine ∪ O.crashed).card ≤ pOpt Replica →  -- ACTUAL faults fit pOpt,
    Synchronised U.toBlockUniverse R →   -- all correct synchronised from R,
    R ≤ S.slotRound k →                  -- the wave lies at or after R,
    Populated U.toBlockUniverse (S.slotRound k) →        -- correct fill the propose round ...
    Populated U.toBlockUniverse (S.slotRound k + 1) →    -- ... and the voting round,
    S.leader k ∈ (Correct : Finset Replica) →            -- and the leader is correct:
    ∃ L, IsLeaderBlock U.toBlockUniverse k L ∧           -- then a candidate exists ...
      FastCommitOpt U.toBlockUniverse L (S.slotRound k)  -- ... and it fast-commits

end DirectLiveness

end Optimal

end Hydrozoan
