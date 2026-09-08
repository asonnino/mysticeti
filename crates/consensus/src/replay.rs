// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Counterfactual replay of the agreed window for the adaptive period.
//!
//! The window is the causal history of the interval's chain anchor (the
//! earliest chain-committed round of the interval, `Committer::complete_scans`)
//! restricted to the last `interval` rounds — agreed data at every honest
//! validator, whether or not the output has reached it. Each candidate period
//! reinterprets the window (`wl'(r) = w_async` iff `r % p' == 0`) and is scored
//! by the expected delay from a round to the output of its blocks; the
//! committer adopts the argmin with hysteresis for the next interval.
//!
//! Purity is load-bearing: everything here is a function of the collected
//! [`Window`] alone — no [`BlockReader`] access after collection — so all
//! honest validators compute identical scores and period schedules. Two
//! documented approximations, both deterministic and uniform across
//! candidates: the indirect rule checks for a certificate anywhere in the
//! window (the window is the real anchor's history) instead of within the
//! replayed anchor's own history, and a skipping candidate contributes the
//! window top as its commit round (deferring its rounds' output upward).
//!
//! The canary leader wait writes the sync rule's evidence into the DAG on
//! async rounds, so only canaried rounds carry manufactured evidence. The
//! replay probes a candidate's canaried sync slots exactly and extrapolates
//! the probes' success rate to its remaining sync slots; probe successes are
//! certificates, so an adversary can only suppress them (under-adopting
//! sync, never over-adopting it). With no probes in the window the replay
//! falls back to exact whole-window evidence.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    sync::Arc,
};

use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::{Committee, StakeAggregator},
    data::Data,
    storage::BlockReader,
};

use crate::protocol::Protocol;

/// The committed window: the anchor's causal history over the last
/// `interval` rounds, indexed per round. Collected once; the replay never
/// touches storage afterwards.
pub struct Window {
    floor: RoundNumber,
    top: RoundNumber,
    /// Blocks per round (index `round - floor`), sorted for determinism.
    rounds: Vec<Vec<Data<Block>>>,
}

/// Everything the replay needs besides the window itself; snapshotted from
/// [`Protocol`] at committer construction.
pub struct ReplayParams {
    pub committee: Arc<Committee>,
    pub direct_commit_quorum: u64,
    pub direct_skip_quorum: u64,
    pub certificate_quorum: u64,
    pub merged_certificates: bool,
    pub sync_wave_length: RoundNumber,
    pub async_wave_length: RoundNumber,
    /// The execution-side canary schedule: only canaried rounds carry
    /// manufactured sync evidence, so the replay probes those and
    /// extrapolates to the rest (see [`replay`]).
    pub canary: Option<NonZeroU64>,
}

impl ReplayParams {
    pub fn from_protocol(protocol: &Protocol, committee: Arc<Committee>) -> Option<Self> {
        let schedule = protocol.steelhead?;
        Some(Self {
            committee,
            direct_commit_quorum: protocol.direct_commit_quorum,
            direct_skip_quorum: protocol.direct_skip_quorum,
            certificate_quorum: protocol.certificate_quorum,
            merged_certificates: protocol.merged_certificates,
            sync_wave_length: schedule.sync_wave_length,
            async_wave_length: schedule.async_wave_length,
            canary: schedule.canary,
        })
    }

    /// Whether execution held the leader wait on `round` when it was an
    /// async slot — i.e. whether the round carries manufactured evidence.
    fn is_canaried(&self, round: RoundNumber) -> bool {
        self.canary
            .is_some_and(|canary| round.is_multiple_of(canary.get()))
    }
}

/// Collect the anchor's causal history restricted to the last `interval`
/// rounds. The single place the replay touches storage.
pub fn collect_window(block_reader: &BlockReader, anchor: &Data<Block>, interval: u64) -> Window {
    let top = anchor.round();
    let floor = top.saturating_sub(interval).max(1);
    let mut members: HashSet<BlockReference> = HashSet::new();
    let mut rounds: Vec<Vec<Data<Block>>> = vec![Vec::new(); (top - floor + 1) as usize];

    let mut stack = vec![anchor.clone()];
    members.insert(*anchor.reference());
    while let Some(block) = stack.pop() {
        for reference in block.includes() {
            if reference.round() < floor || !members.insert(*reference) {
                continue;
            }
            let include = block_reader
                .get_block(*reference)
                .expect("The anchor's causal history must be complete");
            stack.push(include);
        }
        rounds[(block.round() - floor) as usize].push(block);
    }
    for round in &mut rounds {
        round.sort_by_key(|block| (block.author(), block.reference().digest));
    }
    Window { floor, top, rounds }
}

impl Window {
    fn blocks_at(&self, round: RoundNumber) -> &[Data<Block>] {
        if round < self.floor || round > self.top {
            return &[];
        }
        &self.rounds[(round - self.floor) as usize]
    }

    /// First-seen support within the window: for each window block at rounds
    /// `target_round..=span_top`, which block of `(author, target_round)` it
    /// supports, mirroring the committer's DFS order via a bottom-up pass.
    fn support_map(
        &self,
        author: Authority,
        target_round: RoundNumber,
        span_top: RoundNumber,
    ) -> HashMap<BlockReference, BlockReference> {
        let mut support: HashMap<BlockReference, BlockReference> = HashMap::new();
        for round in target_round..=span_top.min(self.top) {
            for block in self.blocks_at(round) {
                let own = *block.reference();
                if round == target_round {
                    if block.author() == author {
                        support.insert(own, own);
                    }
                    continue;
                }
                for include in block.includes() {
                    if include.round() < target_round {
                        continue;
                    }
                    if include.author_round() == (author, target_round) {
                        support.insert(own, *include);
                        break;
                    }
                    if let Some(supported) = support.get(include) {
                        support.insert(own, *supported);
                        break;
                    }
                }
            }
        }
        support
    }
}

/// Scaled expected rounds: all values carry a factor `n` (the committee size)
/// so async averages keep sub-round precision; the truncating division of an
/// async slot's candidate sum is deterministic, which is all agreement needs.
type Scaled = u128;

/// Replay the window under `period` and return the total expected output
/// delay (comparable across candidates: same rounds, same scale).
pub fn replay(window: &Window, params: &ReplayParams, period: NonZeroU64) -> Scaled {
    let n = params.committee.len() as u128;
    let scale = |round: RoundNumber| round as u128 * n;
    let top_scaled = scale(window.top);
    let span = (window.top - window.floor + 1) as usize;
    let probes = probe_rate(window, params, period)
        .map(|(succeeded, total)| (succeeded, total - succeeded, total));

    // Pass 1, top-down: expected decision and commit rounds per slot.
    let mut decisions: Vec<Scaled> = vec![top_scaled; span];
    let mut commits: Vec<Scaled> = vec![top_scaled; span];
    for round in (window.floor..=window.top).rev() {
        let index = (round - window.floor) as usize;
        let is_async = round.is_multiple_of(period.get());
        let wave_length = if is_async {
            params.async_wave_length
        } else {
            params.sync_wave_length
        };
        let decision_round = round + wave_length - 1;
        if decision_round > window.top {
            // Decided at the window top: identical penalty for every candidate.
            continue;
        }
        let voting_round = if params.merged_certificates {
            decision_round
        } else {
            decision_round - 1
        };

        if !is_async
            && !params.is_canaried(round)
            && let Some((succeeded, failed, total)) = probes
        {
            // Un-probed sync slot: only canaried rounds carry manufactured
            // evidence, so extrapolate from the probes — commit at the
            // decision round with the probes' success rate, otherwise read
            // as a direct skip (decided at the voting round, output
            // deferred upward).
            decisions[index] =
                (succeeded * scale(decision_round) + failed * scale(voting_round)) / total;
            commits[index] = (succeeded * scale(decision_round) + failed * top_scaled) / total;
            continue;
        }

        // The replayed anchor: the earliest higher slot with commit mass.
        let anchor = (round + wave_length..=window.top)
            .map(|anchor_round| (anchor_round - window.floor) as usize)
            .find(|&anchor_index| commits[anchor_index] < top_scaled)
            .map(|anchor_index| (decisions[anchor_index], commits[anchor_index]))
            .unwrap_or((top_scaled, top_scaled));

        let candidates: Vec<Authority> = if is_async {
            params.committee.authorities().collect()
        } else {
            // Known leader, offset 0: the round-robin schedule.
            vec![Authority::new(round % params.committee.len() as u64)]
        };
        let mut decision_sum: Scaled = 0;
        let mut commit_sum: Scaled = 0;
        let divisor = candidates.len() as u128;
        for author in candidates {
            let (decision, commit) = replay_slot(
                window,
                params,
                author,
                round,
                voting_round,
                decision_round,
                anchor,
                top_scaled,
            );
            decision_sum += decision;
            commit_sum += commit;
        }
        decisions[index] = decision_sum / divisor;
        commits[index] = commit_sum / divisor;
    }

    // Pass 2, top-down: a round is output at the first commit at or above it.
    let mut output_at = vec![top_scaled; span];
    let mut running = top_scaled;
    for index in (0..span).rev() {
        running = running.min(commits[index]);
        output_at[index] = running;
    }

    // Pass 3, bottom-up: output waits for every lower slot's decision.
    let mut score: Scaled = 0;
    let mut gate: Scaled = 0;
    for (index, round) in (window.floor..=window.top).enumerate() {
        score += output_at[index].max(gate) - scale(round);
        gate = gate.max(decisions[index]);
    }
    score
}

/// Replay one candidate leader of one slot: direct commit, direct skip, or
/// indirect by the anchor. Returns scaled `(decision, commit)` rounds; a slot
/// that never commits contributes the window top (its rounds defer upward).
#[allow(clippy::too_many_arguments)]
fn replay_slot(
    window: &Window,
    params: &ReplayParams,
    author: Authority,
    round: RoundNumber,
    voting_round: RoundNumber,
    decision_round: RoundNumber,
    anchor: (Scaled, Scaled),
    top_scaled: Scaled,
) -> (Scaled, Scaled) {
    let n = params.committee.len() as u128;
    let scale = |round: RoundNumber| round as u128 * n;

    // Deterministic representative among equivocating leader blocks (min
    // digest); a Byzantine window must never panic here.
    let leader_block = window
        .blocks_at(round)
        .iter()
        .filter(|block| block.author() == author)
        .min_by_key(|block| block.reference().digest);

    let support = window.support_map(author, round, decision_round);
    let leader_reference = leader_block.map(|block| *block.reference());

    // Blames: voting-round window blocks supporting no block of this slot.
    let mut blames = StakeAggregator::new(params.direct_skip_quorum);
    let mut blame_quorum = false;
    for block in window.blocks_at(voting_round) {
        if !support.contains_key(block.reference()) && blames.add(block.author(), &params.committee)
        {
            blame_quorum = true;
            break;
        }
    }
    if blame_quorum {
        return (scale(voting_round), top_scaled);
    }

    let Some(leader_reference) = leader_reference else {
        // No block and no blame quorum in the window: undecided, anchor rules.
        return anchor;
    };

    // Direct commit evidence at the decision round.
    if direct_commit_evidence(
        window,
        params,
        &support,
        leader_reference,
        voting_round,
        decision_round,
    ) {
        return (scale(decision_round), scale(decision_round));
    }

    // Indirect: the anchor decides; committed iff the window holds a
    // certificate for this leader (the window is the real anchor's history).
    let is_vote = |block: &Data<Block>| support.get(block.reference()) == Some(&leader_reference);
    let certified = if params.merged_certificates {
        let mut votes = StakeAggregator::new(params.certificate_quorum);
        window
            .blocks_at(decision_round)
            .iter()
            .any(|block| is_vote(block) && votes.add(block.author(), &params.committee))
    } else {
        window.blocks_at(decision_round).iter().any(|block| {
            let mut votes = StakeAggregator::new(params.certificate_quorum);
            block.includes().iter().any(|include| {
                include.round() == voting_round
                    && support.get(include) == Some(&leader_reference)
                    && votes.add(include.authority, &params.committee)
            })
        })
    };
    let (anchor_decision, anchor_commit) = anchor;
    if certified {
        (anchor_decision, anchor_commit)
    } else {
        (anchor_decision, top_scaled)
    }
}

/// Whether the window holds the direct-commit pattern for `leader_reference`:
/// a `direct_commit_quorum` of decision-round blocks that are votes (merged
/// certificates) or that carry a `certificate_quorum` of votes.
fn direct_commit_evidence(
    window: &Window,
    params: &ReplayParams,
    support: &HashMap<BlockReference, BlockReference>,
    leader_reference: BlockReference,
    voting_round: RoundNumber,
    decision_round: RoundNumber,
) -> bool {
    let is_vote = |block: &Data<Block>| support.get(block.reference()) == Some(&leader_reference);
    if params.merged_certificates {
        let mut votes = StakeAggregator::new(params.direct_commit_quorum);
        window
            .blocks_at(decision_round)
            .iter()
            .any(|block| is_vote(block) && votes.add(block.author(), &params.committee))
    } else {
        let mut carriers = StakeAggregator::new(params.direct_commit_quorum);
        window.blocks_at(decision_round).iter().any(|block| {
            let mut votes = StakeAggregator::new(params.certificate_quorum);
            let carries = block.includes().iter().any(|include| {
                include.round() == voting_round
                    && support.get(include) == Some(&leader_reference)
                    && votes.add(include.authority, &params.committee)
            });
            carries && carriers.add(block.author(), &params.committee)
        })
    }
}

/// Probe the canaried sync slots of candidate `period`: those rounds carried
/// the execution-side leader wait, so their direct-commit evidence is honest.
/// Returns `(succeeded, total)` probes, or `None` when the window holds no
/// probe for this candidate (no canary, or every canaried round lands on the
/// candidate's async slots).
fn probe_rate(window: &Window, params: &ReplayParams, period: NonZeroU64) -> Option<(u128, u128)> {
    let n = params.committee.len() as u64;
    let mut succeeded: u128 = 0;
    let mut total: u128 = 0;
    for round in window.floor..=window.top {
        if round.is_multiple_of(period.get()) || !params.is_canaried(round) {
            continue;
        }
        let decision_round = round + params.sync_wave_length - 1;
        if decision_round > window.top {
            continue;
        }
        let voting_round = if params.merged_certificates {
            decision_round
        } else {
            decision_round - 1
        };
        let author = Authority::new(round % n);
        let support = window.support_map(author, round, decision_round);
        let leader_reference = window
            .blocks_at(round)
            .iter()
            .filter(|block| block.author() == author)
            .min_by_key(|block| block.reference().digest)
            .map(|block| *block.reference());
        total += 1;
        if let Some(leader_reference) = leader_reference
            && direct_commit_evidence(
                window,
                params,
                &support,
                leader_reference,
                voting_round,
                decision_round,
            )
        {
            succeeded += 1;
        }
    }
    (total > 0).then_some((succeeded, total))
}

/// Score every candidate period (powers of two up to `max_period`) and pick
/// the argmin, keeping `current` unless the best improves by more than the
/// hysteresis. Ties prefer the current period, then the larger candidate.
pub fn choose_period(
    window: &Window,
    params: &ReplayParams,
    current: NonZeroU64,
    max_period: NonZeroU64,
    epsilon_percent: u8,
) -> NonZeroU64 {
    let candidates = std::iter::successors(NonZeroU64::new(1), |p| {
        p.checked_mul(NonZeroU64::new(2).unwrap())
            .filter(|p| *p <= max_period)
    });

    let mut best: Option<(Scaled, NonZeroU64)> = None;
    let mut current_score: Option<Scaled> = None;
    for candidate in candidates {
        let score = replay(window, params, candidate);
        tracing::debug!(
            "replay window top {} candidate {candidate} score {score}",
            window.top
        );
        if candidate == current {
            current_score = Some(score);
        }
        best = match best {
            Some((best_score, _)) if best_score < score => best,
            Some((best_score, best_period)) if best_score == score && best_period > candidate => {
                best
            }
            _ => Some((score, candidate)),
        };
    }

    let (best_score, best_period) = best.expect("at least one candidate");
    let Some(current_score) = current_score else {
        return best_period;
    };
    if best_score * 100 < current_score * (100 - epsilon_percent as u128) {
        best_period
    } else {
        current
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        sync::Arc,
    };

    use dag::{
        authority::Authority,
        block::{BlockReference, RoundNumber},
        committee::Committee,
        storage::Storage,
        test_util::{build_dag, build_dag_layer, committee, drop_leader},
    };

    use super::{ReplayParams, Window, choose_period, collect_window, replay};
    use crate::protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair};

    const INTERVAL: u64 = 32;
    const MAX_PERIOD: u64 = 8;

    fn params(committee: &Arc<Committee>) -> ReplayParams {
        params_with_canary(committee, NonZeroU64::new(1))
    }

    fn params_with_canary(committee: &Arc<Committee>, canary: Option<NonZeroU64>) -> ReplayParams {
        let spec = ConsensusProtocol::Steelhead {
            pair: SteelheadPair::MysticetiMahiMahi,
            period: None,
            async_wave_length: 5,
            adaptive: Some(AdaptiveConfig {
                interval: INTERVAL,
                max_period: NonZeroU64::new(MAX_PERIOD).unwrap(),
                epsilon_percent: 10,
            }),
            canary,
            leader_count: NonZeroUsize::new(1).unwrap(),
        };
        let protocol = spec.to_protocol(committee).expect("valid protocol");
        ReplayParams::from_protocol(&protocol, committee.clone()).expect("steelhead protocol")
    }

    fn window_at(storage: &Storage, anchor: BlockReference) -> Window {
        let anchor = storage.block_reader().get_block(anchor).expect("anchor");
        collect_window(storage.block_reader(), &anchor, INTERVAL)
    }

    fn period(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    /// Build a DAG where every round's blocks exclude the previous round's
    /// round-robin leader block: the targeted-delay attack shape. The starved
    /// blocks exist in storage but never enter the anchor's causal history.
    fn build_leader_starved_dag(
        committee: &Arc<Committee>,
        storage: &mut Storage,
        depth: RoundNumber,
    ) -> Vec<BlockReference> {
        let n = committee.len() as u64;
        let mut references = build_dag(committee, storage, None, 0);
        for round in 1..=depth {
            let parents = if round == 1 {
                references.clone()
            } else {
                drop_leader(&references, Authority::new((round - 1) % n))
            };
            references = build_dag_layer(
                committee
                    .authorities()
                    .map(|authority| (authority, parents.clone()))
                    .collect(),
                storage,
            );
        }
        references
    }

    #[test]
    fn healthy_window_prefers_the_largest_period() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_dag(&committee, &mut storage, None, 33);
        let window = window_at(&storage, top[0]);
        let params = params(&committee);

        let scores: Vec<_> = [1, 2, 4, 8]
            .map(|candidate| replay(&window, &params, period(candidate)))
            .to_vec();
        assert!(
            scores.windows(2).all(|pair| pair[1] < pair[0]),
            "healthy scores must strictly decrease with the period: {scores:?}"
        );

        let max_period = period(MAX_PERIOD);
        let chosen = choose_period(&window, &params, period(1), max_period, 10);
        assert_eq!(
            chosen, max_period,
            "healthy replay must adopt the largest period"
        );
        let chosen = choose_period(&window, &params, max_period, max_period, 10);
        assert_eq!(
            chosen, max_period,
            "the largest period must be stable when healthy"
        );
    }

    #[test]
    fn leader_starved_window_prefers_period_one() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_leader_starved_dag(&committee, &mut storage, 33);
        let window = window_at(&storage, top[0]);
        let params = params(&committee);

        let scores: Vec<_> = [1, 2, 4, 8]
            .map(|candidate| replay(&window, &params, period(candidate)))
            .to_vec();
        assert!(
            scores.windows(2).all(|pair| pair[0] < pair[1]),
            "starved scores must strictly increase with the period: {scores:?}"
        );

        let max_period = period(MAX_PERIOD);
        let chosen = choose_period(&window, &params, max_period, max_period, 10);
        assert_eq!(
            chosen,
            period(1),
            "a starved replay must fall back to every-round async"
        );
    }

    #[test]
    fn hysteresis_holds_the_current_period() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_dag(&committee, &mut storage, None, 33);
        let window = window_at(&storage, top[0]);
        let params = params(&committee);

        let max_period = period(MAX_PERIOD);
        // A near-total hysteresis threshold pins the current period even
        // though the largest candidate scores far better.
        let chosen = choose_period(&window, &params, period(1), max_period, 99);
        assert_eq!(chosen, period(1));
        // Without hysteresis, any strict improvement switches.
        let chosen = choose_period(&window, &params, period(1), max_period, 0);
        assert_eq!(chosen, max_period);
    }

    #[test]
    fn degenerate_window_keeps_the_current_period() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        // A window too shallow for any slot to decide: every candidate scores
        // identically (zero), so the period must not move.
        let top = build_dag(&committee, &mut storage, None, 1);
        let window = window_at(&storage, top[0]);
        let params = params(&committee);

        for current in [1, 2, MAX_PERIOD] {
            let chosen = choose_period(&window, &params, period(current), period(MAX_PERIOD), 10);
            assert_eq!(chosen, period(current));
        }
    }

    #[test]
    fn equivocating_leaders_do_not_panic() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let round_3 = build_dag(&committee, &mut storage, None, 3);

        // Authority 2 equivocates at round 4 (two blocks, distinct parents);
        // every round-5 block references both. (Authority 0 is the storage's
        // own authority, whose store rejects duplicate rounds.)
        let equivocator = Authority::new(2);
        let mut connections = vec![
            (equivocator, round_3.clone()),
            (equivocator, drop_leader(&round_3, Authority::new(1))),
        ];
        connections.extend(
            committee
                .authorities()
                .filter(|authority| *authority != equivocator)
                .map(|authority| (authority, round_3.clone())),
        );
        let round_4 = build_dag_layer(connections, &mut storage);
        let round_5 = build_dag_layer(
            committee
                .authorities()
                .map(|authority| (authority, round_4.clone()))
                .collect(),
            &mut storage,
        );
        let top = build_dag(&committee, &mut storage, Some(round_5), 12);

        let window = window_at(&storage, top[0]);
        let params = params(&committee);
        for candidate in [1, 2, 4, 8] {
            replay(&window, &params, period(candidate));
        }
        choose_period(&window, &params, period(MAX_PERIOD), period(MAX_PERIOD), 10);
    }

    /// Sparse-canary DAG: the previous round's leader is referenced only
    /// when that round was canaried (multiple of `canary`); starved
    /// otherwise. Models execution at period 1 with a sampled canary.
    fn build_sampled_canary_dag(
        committee: &Arc<Committee>,
        storage: &mut Storage,
        depth: RoundNumber,
        canary: u64,
    ) -> Vec<BlockReference> {
        let n = committee.len() as u64;
        let mut references = build_dag(committee, storage, None, 0);
        for round in 1..=depth {
            let previous = round - 1;
            let parents = if previous == 0 || previous.is_multiple_of(canary) {
                references.clone()
            } else {
                drop_leader(&references, Authority::new(previous % n))
            };
            references = build_dag_layer(
                committee
                    .authorities()
                    .map(|authority| (authority, parents.clone()))
                    .collect(),
                storage,
            );
        }
        references
    }

    #[test]
    fn sparse_canary_probes_enable_the_climb() {
        // Healthy network executed with canary 5: only every 5th round's
        // leader is referenced, yet the probe-aware replay must adopt the
        // largest period from those probes alone.
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_sampled_canary_dag(&committee, &mut storage, 33, 5);
        let window = window_at(&storage, top[0]);

        let probed = params_with_canary(&committee, NonZeroU64::new(5));
        let chosen = choose_period(&window, &probed, period(1), period(MAX_PERIOD), 10);
        assert_eq!(chosen, period(MAX_PERIOD), "probes must certify the climb");
    }

    #[test]
    fn starved_probes_stay_async() {
        // Fully starved DAG (no leader ever referenced): probes fail, so the
        // sampled canary must not climb.
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_leader_starved_dag(&committee, &mut storage, 33);
        let window = window_at(&storage, top[0]);
        let probed = params_with_canary(&committee, NonZeroU64::new(5));
        let chosen = choose_period(&window, &probed, period(MAX_PERIOD), period(MAX_PERIOD), 10);
        assert_eq!(chosen, period(1));
    }

    #[test]
    fn window_excludes_blocks_outside_the_anchor_history() {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        let top = build_leader_starved_dag(&committee, &mut storage, 10);

        let anchor = storage.block_reader().get_block(top[0]).expect("anchor");
        let window = collect_window(storage.block_reader(), &anchor, INTERVAL);
        // Each round below the top misses exactly the starved leader block.
        let n = committee.len() as u64;
        for round in 1..10 {
            let authors: Vec<_> = window
                .blocks_at(round)
                .iter()
                .map(|block| block.author())
                .collect();
            assert_eq!(
                authors.len(),
                committee.len() - 1,
                "round {round}: {authors:?}"
            );
            assert!(
                !authors.contains(&Authority::new(round % n)),
                "round {round}"
            );
        }
    }
}
