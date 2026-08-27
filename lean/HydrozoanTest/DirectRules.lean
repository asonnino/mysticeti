import Hydrozoan.Helpers.Schedule
import Hydrozoan.Helpers.DirectRules
import HydrozoanTest.CausalHistory

/-!
# Witness: the direct rules fire

The direct rules evaluated on the three-round universe `U2` under a
pipelined single-leader schedule (slot `k` at round `k`, led by replica
`(k + 2) % 7`). Slot 0 is round 0 led by replica 2, whose candidate is
genesis id 2 — every round-1 block votes for it, so the fast path fires
exactly at quorum; id 14 certifies it, but one certificate is far short
of `q_slow`, so the slow path does not — "a fast commit frequently
leaves no slow path", made concrete. Thresholds at
`n = 7, f = c = k = 1`: `q_fast = 6`, `q_cert = 5`, `q_slow = 4`.
-/

namespace HydrozoanTest

open Hydrozoan

/-- The pipelined schedule: slot `k` at round `k`, led by replica
`(k + 2) % 7` — slot 0 by replica 2, slot 1 by replica 3, ... -/
instance : Slots (Fin 7) :=
  Slots.uniformSingle 1 (by omega) fun k => ⟨(k + 2) % 7, by omega⟩

-- Slot arithmetic under the pipelined schedule.
example : votingRound (Fin 7) 0 = 1 ∧ decisionRound (Fin 7) 0 = 2 := by decide

-- Genesis id 2 is slot 0's candidate. Id 3 is not (right round, wrong
-- author); the equivocating id 7 is not either (wrong round and wrong
-- author).
example : IsLeaderBlock U2 0 2 := by decide
example : ¬ IsLeaderBlock U2 0 3 := by decide
example : ¬ IsLeaderBlock U2 0 7 := by decide

-- Every round-1 block votes for id 2 (it sits in every parent set) ...
example : IsVote U2 7 2 ∧ IsVote U2 8 2 ∧ IsVote U2 9 2 := by decide

-- ... so slot 0's candidate is fast-committed exactly at quorum: six
-- distinct supporters (the equivocator counted once, crashed replica 1
-- silent).
example : supporters U2 2 1 = {0, 2, 3, 4, 5, 6} := by decide
example : FastCommit U2 2 0 := by decide

-- The view V2 misses id 11, so it sees only five supporters: a view can
-- under-report a fast commit (the safe direction), never invent one.
example : ¬ FastCommitInView U2 V2 2 0 := by decide

-- Id 14 certifies id 2: its five parents all vote for 2, from exactly
-- q_cert distinct authors.
example : voteBlocks U2 14 2 = {7, 9, 10, 12, 13} := by decide
example : IsCertificate U2 14 2 := by decide

-- But one certifier is far short of q_slow = 4: the slow path has not
-- formed at this point of the execution. (Temporal, not structural: any
-- further valid round-2 block here WOULD certify id 2. The structural
-- fast-commit-with-no-possible-certificate witness arrives with the
-- slot-safety phase.)
example : certificates U2 2 0 = {14} := by decide
example : certifiers U2 2 0 = {2} := by decide
example : ¬ SlowCommit U2 2 0 := by decide

-- Nobody blames slot 0 — a fast-committed leader gathers no skip
-- quorum.
example : blames U2 0 = ∅ := by decide
example : ¬ SkippedLeader U2 0 := by decide

-- Slot 1 (round 1, led by replica 3): its candidate id 10 has a single
-- voter at round 2 (id 14), far below q_fast — no fast commit.
example : IsLeaderBlock U2 1 10 := by decide
example : supporters U2 10 2 = {2} := by decide
example : ¬ FastCommit U2 10 1 := by decide

end HydrozoanTest
