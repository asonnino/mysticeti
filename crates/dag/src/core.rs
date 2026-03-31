// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashSet, VecDeque},
    mem,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{
    block_handler::RealBlockHandler,
    block_manager::BlockManager,
    block_store::{CommitData, OwnBlockData},
    committee::Committee,
    config::{NodePrivateConfig, NodePublicConfig},
    consensus::{
        linearizer::CommittedSubDag,
        universal_committer::{UniversalCommitter, UniversalCommitterBuilder},
    },
    context::Ctx,
    crypto::Signer,
    data::Data,
    epoch_close::EpochManager,
    metrics::Metrics,
    runtime::timestamp_utc,
    state::RecoveredState,
    storage::BlockReader,
    storage::Storage,
    threshold_clock::ThresholdClockAggregator,
    types::{AuthorityIndex, BaseStatement, BlockReference, RoundNumber, StatementBlock},
    wal::{WalPosition, WalSyncer},
};

pub struct Core<C: Ctx> {
    block_manager: BlockManager,
    pending: VecDeque<(WalPosition, MetaStatement)>,
    last_own_block: OwnBlockData,
    block_handler: RealBlockHandler<C>,
    authority: AuthorityIndex,
    threshold_clock: ThresholdClockAggregator,
    pub(crate) committee: Arc<Committee>,
    last_commit_leader: BlockReference,
    storage: Storage,
    pub(crate) metrics: Arc<Metrics>,
    options: CoreOptions,
    signer: Signer,
    // todo - ugly, probably need to merge syncer and core
    recovered_committed_blocks: Option<HashSet<BlockReference>>,
    epoch_manager: EpochManager,
    rounds_in_epoch: RoundNumber,
    committer: UniversalCommitter,
}

pub struct CoreOptions {
    fsync: bool,
}

#[derive(Debug)]
pub enum MetaStatement {
    Include(BlockReference),
    Payload(Vec<BaseStatement>),
}

impl<C: Ctx> Core<C> {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        block_handler: RealBlockHandler<C>,
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        private_config: NodePrivateConfig,
        public_config: &NodePublicConfig,
        metrics: Arc<Metrics>,
        mut storage: Storage,
        recovered: RecoveredState,
        options: CoreOptions,
    ) -> Self {
        let RecoveredState {
            last_own_block,
            mut pending,
            unprocessed_blocks,
            last_committed_leader,
            committed_blocks,
        } = recovered;
        let mut threshold_clock = ThresholdClockAggregator::new(0);
        let last_own_block = if let Some(own_block) = last_own_block {
            for (_, pending_block) in pending.iter() {
                if let MetaStatement::Include(include) = pending_block {
                    threshold_clock.add_block(*include, &committee);
                }
            }
            own_block
        } else {
            // todo(fix) - this technically has a race condition if node crashes after genesis
            assert!(pending.is_empty());
            // Initialize empty block store
            // A lot of this code is shared with Self::add_blocks,
            // this is not great and some code reuse would be great
            let (own_genesis_block, other_genesis_blocks) = committee.genesis_blocks(authority);
            assert_eq!(own_genesis_block.author(), authority);
            for block in other_genesis_blocks {
                let reference = *block.reference();
                threshold_clock.add_block(reference, &committee);
                let position = storage.insert_block(block);
                pending.push_back((position, MetaStatement::Include(reference)));
            }
            threshold_clock.add_block(*own_genesis_block.reference(), &committee);
            let own_block_data = OwnBlockData {
                next_entry: WalPosition::MAX,
                block: own_genesis_block,
            };
            storage.insert_own_block(&own_block_data);
            own_block_data
        };
        let block_manager = BlockManager::new(storage.block_reader().clone(), &committee);

        let epoch_manager = EpochManager::new();

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            storage.block_reader().clone(),
            metrics.clone(),
        )
        .with_number_of_leaders(public_config.parameters.number_of_leaders)
        .with_pipeline(public_config.parameters.enable_pipelining)
        .build();
        tracing::info!(
            "Pipeline enabled: {}",
            public_config.parameters.enable_pipelining
        );
        tracing::info!(
            "Number of leaders: {}",
            public_config.parameters.number_of_leaders
        );

        let mut this = Self {
            block_manager,
            pending,
            last_own_block,
            block_handler,
            authority,
            threshold_clock,
            committee,
            last_commit_leader: last_committed_leader.unwrap_or_default(),
            storage,
            metrics,
            options,
            signer: private_config.keypair,
            recovered_committed_blocks: Some(committed_blocks),
            epoch_manager,
            rounds_in_epoch: public_config.parameters.rounds_in_epoch,
            committer,
        };

        if !unprocessed_blocks.is_empty() {
            tracing::info!(
                "Replaying {} blocks for transaction aggregator",
                unprocessed_blocks.len()
            );
            this.run_block_handler();
        }

        this
    }

    pub fn with_options(mut self, options: CoreOptions) -> Self {
        self.options = options;
        self
    }

    // Note that generally when you update this function you
    // also want to change genesis initialization above
    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) -> Vec<Data<StatementBlock>> {
        let _timer = self.metrics.utilization_timer("Core::add_blocks");
        let processed = self.block_manager.add_blocks(blocks, &mut self.storage);
        let mut result = Vec::with_capacity(processed.len());
        for (position, processed) in processed.into_iter() {
            self.threshold_clock
                .add_block(*processed.reference(), &self.committee);
            self.pending
                .push_back((position, MetaStatement::Include(*processed.reference())));
            result.push(processed);
        }
        self.run_block_handler();
        result
    }

    fn run_block_handler(&mut self) {
        let _timer = self.metrics.utilization_timer("Core::run_block_handler");
        let statements = self.block_handler.handle_blocks(!self.epoch_changing());
        let serialized_statements =
            bincode::serialize(&statements).expect("Payload serialization failed");
        let position = self.storage.write_payload(&serialized_statements);
        self.pending
            .push_back((position, MetaStatement::Payload(statements)));
    }

    pub fn try_new_block(&mut self) -> Option<Data<StatementBlock>> {
        let _timer = self.metrics.utilization_timer("Core::try_new_block");
        let clock_round = self.threshold_clock.get_round();
        if clock_round <= self.last_proposed() {
            return None;
        }

        let mut includes = vec![];
        let mut statements = vec![];

        let first_include_index = self
            .pending
            .iter()
            .position(|(_, statement)| match statement {
                MetaStatement::Include(block_ref) => block_ref.round >= clock_round,
                _ => false,
            })
            .unwrap_or(self.pending.len());

        let mut taken = self.pending.split_off(first_include_index);
        // Split off returns the "tail", what we want is keep the tail in "pending" and get the head
        mem::swap(&mut taken, &mut self.pending);
        // Compress the references in the block
        // Iterate through all the include statements in the block,
        // and make a set of all the references in their includes.
        let mut references_in_block: HashSet<BlockReference> = HashSet::new();
        references_in_block.extend(self.last_own_block.block.includes());
        for (_, statement) in &taken {
            if let MetaStatement::Include(block_ref) = statement {
                // for all the includes in the block, add the references in the block to the set
                if let Some(block) = self.storage.block_reader().get_block(*block_ref) {
                    references_in_block.extend(block.includes());
                }
            }
        }
        includes.push(*self.last_own_block.block.reference());
        for (_, statement) in taken.into_iter() {
            match statement {
                MetaStatement::Include(include) => {
                    if !references_in_block.contains(&include) {
                        includes.push(include);
                    }
                }
                MetaStatement::Payload(payload) => {
                    if !self.epoch_changing() {
                        statements.extend(payload);
                    }
                }
            }
        }

        assert!(!includes.is_empty());
        let time_ns = timestamp_utc().as_nanos();
        let block = StatementBlock::new_with_signer(
            self.authority,
            clock_round,
            includes,
            statements,
            time_ns,
            self.epoch_changing(),
            &self.signer,
        );
        assert_eq!(
            block.includes().first().unwrap().authority,
            self.authority,
            "Invalid block {}",
            block
        );

        let block = Data::new(block);
        if block.serialized_bytes().len() > crate::storage::wal::MAX_ENTRY_SIZE / 2 {
            panic!(
                "Created an oversized block \
                (check all limits set properly: {} > {}): {:?}",
                block.serialized_bytes().len(),
                crate::storage::wal::MAX_ENTRY_SIZE / 2,
                block.detailed()
            );
        }
        self.threshold_clock
            .add_block(*block.reference(), &self.committee);
        self.block_handler.handle_proposal(&block);
        self.proposed_block_stats(&block);
        let next_entry = if let Some((pos, _)) = self.pending.front() {
            *pos
        } else {
            WalPosition::MAX
        };
        self.last_own_block = OwnBlockData {
            next_entry,
            block: block.clone(),
        };
        self.storage.insert_own_block(&self.last_own_block);

        if self.options.fsync {
            self.storage.sync();
        }

        tracing::debug!("Created block {block:?}");
        Some(block)
    }

    pub fn wal_syncer(&self) -> WalSyncer {
        self.storage.syncer()
    }

    fn proposed_block_stats(&self, block: &Data<StatementBlock>) {
        self.metrics
            .observe_proposed_block_size_bytes(block.serialized_bytes().len());
        let transactions = block.statements().len();
        self.metrics
            .observe_proposed_block_transaction_count(transactions);
        self.metrics.observe_proposed_block_vote_count(0);
    }

    pub fn try_commit(&mut self) -> Vec<Data<StatementBlock>> {
        let sequence: Vec<_> = self
            .committer
            .try_commit(self.last_commit_leader)
            .into_iter()
            .filter_map(|leader| leader.into_decided_block())
            .collect();

        if let Some(last) = sequence.last() {
            self.last_commit_leader = *last.reference();
        }

        // todo: should ideally come from execution result of epoch smart contract
        if self.last_commit_leader.round() > self.rounds_in_epoch {
            self.epoch_manager.epoch_change_begun();
        }

        sequence
    }

    pub fn cleanup(&self) {
        const RETAIN_BELOW_COMMIT_ROUNDS: RoundNumber = 100;

        self.storage.block_reader().cleanup(
            self.last_commit_leader
                .round()
                .saturating_sub(RETAIN_BELOW_COMMIT_ROUNDS),
        );

        self.block_handler.cleanup();
    }

    /// This only checks readiness in terms of helping liveness for commit rule,
    /// try_new_block might still return None if threshold clock is not ready
    ///
    /// The algorithm to calling is roughly:
    /// if timeout || commit_ready_new_block then try_new_block(..)
    pub fn ready_new_block(
        &self,
        period: u64,
        connected_authorities: &HashSet<AuthorityIndex>,
    ) -> bool {
        let quorum_round = self.threshold_clock.get_round();

        // Leader round we check if we have a leader block
        if quorum_round > self.last_commit_leader.round().max(period - 1) {
            let leader_round = quorum_round - 1;
            let mut leaders = self.committer.get_leaders(leader_round);
            leaders.retain(|leader| connected_authorities.contains(leader));
            self.storage
                .block_reader()
                .all_blocks_exists_at_authority_round(&leaders, leader_round)
        } else {
            false
        }
    }

    pub fn handle_committed_subdag(&mut self, committed: Vec<CommittedSubDag>) -> Vec<CommitData> {
        let mut commit_data = vec![];
        for commit in &committed {
            for block in &commit.blocks {
                self.epoch_manager
                    .observe_committed_block(block, &self.committee);
            }
            commit_data.push(CommitData::from(commit));
        }
        self.storage.write_commits(&commit_data);
        // todo - We should also persist state of the epoch manager, otherwise if validator
        // restarts during epoch change it will fork on the epoch change state.
        commit_data
    }

    pub fn take_recovered_committed_blocks(&mut self) -> HashSet<BlockReference> {
        self.recovered_committed_blocks
            .take()
            .expect("take_recovered_committed_blocks called twice")
    }

    pub fn block_reader(&self) -> &BlockReader {
        self.storage.block_reader()
    }

    pub fn last_own_block(&self) -> &Data<StatementBlock> {
        &self.last_own_block.block
    }

    pub fn last_proposed(&self) -> RoundNumber {
        self.last_own_block.block.round()
    }

    pub fn authority(&self) -> AuthorityIndex {
        self.authority
    }

    pub fn block_handler(&self) -> &RealBlockHandler<C> {
        &self.block_handler
    }

    pub fn block_manager(&self) -> &BlockManager {
        &self.block_manager
    }

    pub fn block_handler_mut(&mut self) -> &mut RealBlockHandler<C> {
        &mut self.block_handler
    }

    pub fn committee(&self) -> &Arc<Committee> {
        &self.committee
    }

    pub fn epoch_closed(&self) -> bool {
        self.epoch_manager.closed()
    }

    pub fn epoch_changing(&self) -> bool {
        self.epoch_manager.changing()
    }

    pub fn epoch_closing_time(&self) -> Arc<AtomicU64> {
        self.epoch_manager.closing_time()
    }
}

impl Default for CoreOptions {
    fn default() -> Self {
        Self::test()
    }
}

impl CoreOptions {
    pub fn test() -> Self {
        Self { fsync: false }
    }

    pub fn production() -> Self {
        Self { fsync: true }
    }
}

#[cfg(test)]
#[cfg(not(feature = "simulator"))]
mod test {
    use rand::{prelude::StdRng, Rng, SeedableRng};

    use super::*;
    use crate::{
        test_util::{committee_and_cores, committee_and_cores_persisted},
        threshold_clock,
    };

    #[test]
    fn test_core_simple_exchange() {
        let (_committee, mut cores) = committee_and_cores(4);

        let mut blocks = vec![];
        for core in &mut cores {
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            eprintln!("{}: {}", core.authority, block);
            blocks.push(block.clone());
        }
        let more_blocks = blocks.split_off(1);

        eprintln!("===");

        let mut blocks_r2 = vec![];
        for core in &mut cores {
            core.add_blocks(blocks.clone());
            assert!(core.try_new_block().is_none());
            core.add_blocks(more_blocks.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 2);
            blocks_r2.push(block.clone());
        }

        for core in &mut cores {
            core.add_blocks(blocks_r2.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 3);
        }
    }

    #[test]
    fn test_randomized_simple_exchange() {
        for seed in 0..100u8 {
            let mut rng = StdRng::from_seed([seed; 32]);
            let (committee, mut cores) = committee_and_cores(4);

            let mut pending: Vec<_> = committee.authorities().map(|_| vec![]).collect();
            for core in &mut cores {
                let block = core
                    .try_new_block()
                    .expect("Must be able to create block after genesis");
                assert_eq!(block.reference().round, 1);
                eprintln!("{}: {}", core.authority, block);
                assert!(
                    threshold_clock::threshold_clock_valid_non_genesis(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                push_all(&mut pending, core.authority, &block);
            }
            let target_round = 10;
            for i in 0..1000 {
                let authority = committee.random_authority(&mut rng);
                let core = &mut cores[authority as usize];
                let this_pending = &mut pending[authority as usize];
                let c = rng.gen_range(1..4usize);
                let mut blocks = vec![];
                for _ in 0..c {
                    if this_pending.is_empty() {
                        break;
                    }
                    let block = this_pending.remove(rng.gen_range(0..this_pending.len()));
                    blocks.push(block);
                }
                if blocks.is_empty() {
                    continue;
                }
                core.add_blocks(blocks);
                let Some(block) = core.try_new_block() else {
                    continue;
                };
                assert!(
                    threshold_clock::threshold_clock_valid_non_genesis(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                push_all(&mut pending, core.authority, &block);
                if cores.iter().all(|c| c.last_proposed() >= target_round) {
                    println!(
                        "Seed {seed} succeed in {i} exchanges, \
                        all cores reached round {target_round}",
                    );
                    break;
                }
            }
            assert!(
                cores.iter().all(|c| c.last_proposed() >= target_round),
                "Seed {seed} failed - not all cores reached \
                round {target_round}",
            );
        }
    }

    #[test]
    fn test_core_recovery() {
        let tmp = tempdir::TempDir::new("test_core_recovery").unwrap();
        let (_committee, mut cores) = committee_and_cores_persisted(4, Some(tmp.path()));

        let mut blocks = vec![];
        for core in &mut cores {
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            eprintln!("{}: {}", core.authority, block);
            blocks.push(block.clone());
        }
        drop(cores);

        let (_committee, mut cores) = committee_and_cores_persisted(4, Some(tmp.path()));

        let more_blocks = blocks.split_off(2);

        eprintln!("===");

        let mut blocks_r2 = vec![];
        for core in &mut cores {
            core.add_blocks(blocks.clone());
            assert!(core.try_new_block().is_none());
            core.add_blocks(more_blocks.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 2);
            blocks_r2.push(block.clone());
        }

        // Do not call Core::write_state here — recovery should
        // handle this by re-processing unprocessed_blocks.
        drop(cores);

        eprintln!("===");

        let (_committee, mut cores) = committee_and_cores_persisted(4, Some(tmp.path()));

        for core in &mut cores {
            core.add_blocks(blocks_r2.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 3);
        }
    }

    fn push_all(
        p: &mut [Vec<Data<StatementBlock>>],
        except: AuthorityIndex,
        block: &Data<StatementBlock>,
    ) {
        for (i, q) in p.iter_mut().enumerate() {
            if i as AuthorityIndex != except {
                q.push(block.clone());
            }
        }
    }
}
