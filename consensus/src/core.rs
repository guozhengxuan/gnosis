use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::*;
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use crypto::{Digest, PublicKey, SignatureService};
use crypto::{Hash as _, Signature};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

#[cfg(test)]
#[path = "tests/smvba_tests.rs"]
pub mod smvba_tests;

pub type SeqNumber = u64; // For both round and view

pub const OPT: u8 = 0;
pub const PES: u8 = 1;
pub const FALLBACK: u8 = 2;

pub const INIT_PHASE: u8 = 0;
pub const LOCK_PHASE: u8 = 1;
pub const FIN_PHASE: u8 = 2;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    HsPropose(Block),
    HSVote(HVote),
    HsLoopBack(Block),
    HsTimeout(Timeout),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block),
    SPBPropose(SPBValue, SPBProof),
    SPBVote(SPBVote),
    SPBFinsh(SPBValue, SPBProof),
    SPBDoneAndShare(MDoneAndShare),
    SMVBAPreVote(MPreVote),
    SMVBAVote(MVote),
    SMVBAHalt(MHalt), //mvba halt
    ParPrePare(PrePare),
    ParLoopBack(Block),
    FBPropose(Block),
    FBVote(HVote),
    FBLoopBack(Block),
}

pub struct Core {
    // Identity, parameters, store, mempool and channels.
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    pk_set: PublicKeySet,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    smvba_channel: Receiver<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    commit_channel: Sender<Block>,
    aggregator: Aggregator,
    opt_path: bool,
    pes_path: bool,

    // Fast path.
    height: SeqNumber, // current height
    last_voted_height: SeqNumber,
    last_committed_height: SeqNumber,
    high_qc: QC,
    timer: Timer,

    // Fallback.
    prepare_tag: HashMap<SeqNumber, bool>, //标记 height高度的 val是否已经发送
    par_prepare_opts: HashMap<SeqNumber, HashMap<PublicKey, Signature>>,
    par_prepare_pess: HashMap<SeqNumber, HashMap<PublicKey, Signature>>,
    fallback_height: SeqNumber, // current fallback height
    last_committed_fallback_height: SeqNumber,
    fallback_length: SeqNumber,
    fallback_high_qc: HashMap<(SeqNumber, SeqNumber), Option<QC>>,

    // sMVBA
    network_filter_smvba: Sender<FilterInput>,
    smvba_y_flag: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_n_flag: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_d_flag: HashMap<(SeqNumber, SeqNumber), bool>, //2f+1 个finish？
    spb_proposes: HashMap<(SeqNumber, SeqNumber), SPBValue>,
    spb_finishs: HashMap<(SeqNumber, SeqNumber), HashMap<PublicKey, (SPBValue, SPBProof)>>,
    spb_locks: HashMap<(SeqNumber, SeqNumber), HashMap<PublicKey, (SPBValue, SPBProof)>>,
    spb_current_phase: HashMap<(SeqNumber, SeqNumber), u8>,
    spb_abandon_flag: HashMap<SeqNumber, bool>,
    smvba_halt_falg: HashMap<SeqNumber, bool>,
    smvba_dones: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>,
    smvba_current_round: HashMap<SeqNumber, SeqNumber>, // height->round
    smvba_votes: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>, // 记录所有的投票数量
    smvba_no_prevotes: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>,
    smvba_is_invoke: HashMap<SeqNumber, bool>,
}
impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        core_channel: Receiver<ConsensusMessage>,
        smvba_channel: Receiver<ConsensusMessage>,
        network_filter: Sender<FilterInput>,
        network_filter_smvba: Sender<FilterInput>,
        commit_channel: Sender<Block>,
        opt_path: bool,
        pes_path: bool,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        let fallback_length = parameters.fallback_length.clone();
        let timer = Timer::new(parameters.timeout_delay);
        let mut core = Self {
            name,
            committee,
            parameters,
            signature_service,
            store,
            pk_set,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_filter,
            network_filter_smvba,
            commit_channel,
            core_channel,
            smvba_channel,
            height: 0,
            last_voted_height: 0,
            last_committed_height: 0,
            high_qc: QC::genesis(),
            timer,
            aggregator,
            opt_path,
            pes_path,
            smvba_y_flag: HashMap::new(),
            smvba_n_flag: HashMap::new(),
            smvba_d_flag: HashMap::new(),
            spb_proposes: HashMap::new(),
            spb_finishs: HashMap::new(),
            spb_locks: HashMap::new(),
            spb_current_phase: HashMap::new(),
            spb_abandon_flag: HashMap::new(),
            smvba_halt_falg: HashMap::new(),
            smvba_current_round: HashMap::new(),
            smvba_dones: HashMap::new(),
            smvba_votes: HashMap::new(),
            smvba_no_prevotes: HashMap::new(),
            smvba_is_invoke: HashMap::new(),
            prepare_tag: HashMap::new(),
            par_prepare_opts: HashMap::new(),
            par_prepare_pess: HashMap::new(),
            fallback_height: 0,
            last_committed_fallback_height: 0,
            fallback_length,
            fallback_high_qc: HashMap::new(),
        };
        core.update_smvba_state(1, 1);
        core.update_prepare_state(1);
        return core;
    }

    fn update_smvba_state(&mut self, height: SeqNumber, round: SeqNumber) {
        //每人都从第一轮开始
        self.smvba_d_flag.insert((height, round), false);
        self.smvba_y_flag.insert((height, round), false);
        self.smvba_n_flag.insert((height, round), false);
        self.smvba_current_round.insert(height, round);
        self.spb_current_phase.insert((height, round), INIT_PHASE);
        self.smvba_dones.insert((height, round), HashSet::new());
        self.smvba_no_prevotes
            .insert((height, round), HashSet::new());
        self.smvba_votes.insert((height, round), HashSet::new());
        self.spb_abandon_flag.remove(&height);
    }

    fn update_prepare_state(&mut self, height: SeqNumber) {
        self.par_prepare_opts.insert(height, HashMap::new());
        self.par_prepare_pess.insert(height, HashMap::new());
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    fn increase_last_voted_height(&mut self, target: SeqNumber) {
        self.last_voted_height = max(self.last_voted_height, target);
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::SyncReply(block);
            Synchronizer::transmit(
                message,
                &self.name,
                Some(&sender),
                &self.network_filter,
                &self.committee,
                OPT,
            )
            .await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn generate_proposal(
        &mut self,
        height: SeqNumber,
        round: SeqNumber,
        qc: Option<QC>,
        tc: Option<TC>,
        tag: u8,
    ) -> Block {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size, tag)
            .await;
        let block = Block::new(
            qc.unwrap_or(QC::genesis()),
            tc,
            self.name,
            height,
            round,
            payload,
            self.signature_service.clone(),
            tag,
        )
        .await;

        if !block.payload.is_empty() {
            info!(
                "Created {} round {} tag {}",
                block, block.round, block.tag
            );

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Created B{}({}) round {} tag {}",
                    block.height,
                    base64::encode(x),
                    block.round,
                    block.tag
                );
            }
        }
        debug!("Created {:?}", block);

        block
    }

    async fn commit(&mut self, block: &Block) -> ConsensusResult<()> {
        let mut current = block.clone();

        // Both opt and pes blocks have the genesis block (opt) as root.
        while current.tag == PES && current.height > self.last_committed_fallback_height ||
            current.tag == OPT && current.height > self.last_committed_height
        {
            if !current.payload.is_empty() {
                info!(
                    "Committed {} round {} tag {}",
                    current, current.round, current.tag
                );

                #[cfg(feature = "benchmark")]
                for x in &current.payload {
                    info!(
                        "Committed B{}({}) round {} tag {}",
                        current.height,
                        base64::encode(x),
                        current.round,
                        current.tag
                    );
                }
                // Cleanup the mempool.
                self.mempool_driver.cleanup_par(&current).await;
            }
            debug!("Committed {}", current);
            let parent = match self.synchronizer.get_parent_block(&current).await? {
                Some(b) => b,
                None => {
                    debug!(
                        "Commit ancestors, processing of {} suspended: missing parent",
                        current.digest()
                    );
                    break;
                }
            };
            current = parent;
        }

        if block.tag == OPT {
            self.last_committed_height = max(self.last_committed_height, block.height);
        } else {
            self.last_committed_fallback_height = max(
                self.last_committed_fallback_height, 
                block.height
            );
        }

        Ok(())
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        if !self.opt_path || self.height == 0 {
            return Ok(())
        }

        // Received block at self.height, timeout for self.height + 1.
        self.advance_height(self.height).await;

        warn!("PBFT Timeout reached for height {}", self.height);

        // Increase the last voted round.
        self.increase_last_voted_height(self.height);

        // Make a timeout message.
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.height,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);

        // Broadcast timeout.
        debug!("Broadcast timeout {:?}", timeout);
        Synchronizer::transmit(
            ConsensusMessage::HsTimeout(timeout.clone()),
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
            OPT,
        )
        .await?;
        
        self.handle_timeout(&timeout).await?;

        Ok(())
    }

    fn update_high_qc(&mut self, qc: &QC) {
        if qc.height > self.high_qc.height {
            self.high_qc = qc.clone();
        }
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.height < self.height {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.update_high_qc(&timeout.high_qc);

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);

            // Try to advance the height.
            self.advance_height(tc.height).await;

            // Propose a new block with tc if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.height) {
                self.opt_propose(self.height, Some(tc)).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_height(&mut self, height: SeqNumber) {
        if height < self.height {
            return;
        }

        // Cleanup vote & timeout aggregator.
        self.aggregator.cleanup_pbft(&self.height);

        // Reset the timer and advance round.
        self.timer.reset();
        self.height = height + 1;
        debug!("Moved to height {}", self.height);

        // Prepare for fallback.
        self.update_prepare_state(self.height);
        self.update_smvba_state(self.height, 1);
    }

    /***********************pbft*************************/

    async fn opt_propose(&mut self, height: SeqNumber, tc: Option<TC>) -> ConsensusResult<()> {
        // Generate a new block and broadcast it.
        let block = self
            .generate_proposal(height, 0, Some(self.high_qc.clone()), tc, OPT)
            .await;

        let message = ConsensusMessage::HsPropose(block.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
            OPT,
        )
        .await?;
        
        // Handle and vote by the node itself.
        self.handle_opt_proposal(&block).await?;

        // Wait for the minimum block delay.
        if !self.parameters.ddos {
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        }

        Ok(())
    }

    async fn handle_opt_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        // Ensure the block is proposed by the leader.
        let digest = block.digest();
        ensure!(
            block.author == self.leader_elector.get_leader(block.height),
            ConsensusError::WrongBlockSender {
                digest,
                name: block.author,
                round: block.height
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Only update high QC.
        self.update_high_qc(&block.qc);

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone(), OPT).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_opt_block(block).await
    }

    #[async_recursion]
    async fn process_opt_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing OPT Block {:?}", block);

        self.store_block(block).await;

        // // Ensure the block's round is as expected.
        // // This check is important: it prevents bad leaders from producing blocks
        // // far in the future that may cause overflow on the round number.
        // if block.height != self.height {
        //     debug!("Exit before vote for opt block at height: {}, node height: {}",
        //         block.height, self.height);
        //     return Ok(());
        // }

        // See if we can broadcast HVote with round=1 (prepare message in PBFT).
        if let Some(vote) = self.make_pbft_prepare(block).await {
            debug!("Created pbft prepare {:?}", vote);
            let message = ConsensusMessage::HSVote(vote.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
                OPT,
            )
            .await?;
            self.handle_opt_vote(&vote).await?;
        }

        Ok(())
    }

    async fn make_pbft_prepare(&mut self, block: &Block) -> Option<HVote> {
        // Check if we can broadcast PBFT's prepare for this block.
        let safety_rule_1 = block.height > self.last_voted_height;
        let mut safety_rule_2 = block.qc.height + 1 == block.height;
        if let Some(ref tc) = block.tc {
            let mut can_extend = tc.height + 1 == block.height;
            can_extend &= block.qc.height >= *tc.high_qc_heights().iter().max().expect("Empty TC");
            safety_rule_2 |= can_extend;
        }
        if !(safety_rule_1 && safety_rule_2) {
            debug!("Failed to vote prepare for block {}", block);
            debug!("rule1: {} rule2: {}", safety_rule_1, safety_rule_2);
            return None;
        }

        // Ensure we won't broadcast PBFT's prepare for contradicting blocks.
        self.increase_last_voted_height(block.height);
        Some(HVote::new(&block, 1, self.name, OPT, self.signature_service.clone()).await)
    }

    async fn make_pbft_commit(&mut self, prepare_qc: &QC) -> Option<HVote> {
        let vote = HVote {
            hash: prepare_qc.hash.clone(),
            height: prepare_qc.height,
            round: 2,
            proposer: prepare_qc.proposer,
            author: self.name,
            tag: OPT,
            signature: Signature::default(),
        };
        let signature = self.signature_service.request_signature(vote.digest()).await;
        Some(HVote { signature, ..vote })
    }

    #[async_recursion]
    async fn handle_opt_vote(&mut self, vote: &HVote) -> ConsensusResult<()> {
        debug!("Processing OPT Vote {:?}", vote);

        if vote.height < self.height {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_pbft_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            if qc.round == 1 {
                // Update high QC.
                self.update_high_qc(&qc);

                // Broadcast HVote with round=2 (commit message in PBFT).
                if let Some(vote) = self.make_pbft_commit(&qc).await {
                    debug!("Created pbft commit {:?}", vote);
                    let message = ConsensusMessage::HSVote(vote.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter,
                        &self.committee,
                        OPT,
                    )
                    .await?;
                    self.handle_opt_vote(&vote).await?;
                }
            } else if qc.round == 2 {
                // This block has been certified by a quorum of PBFT's commit messages.
                // and is ready to output. Now directly input 0 to next DBA.
                if self.pes_path {
                    self.fallback_height += 1;
                    debug!("Moved to fallback height: {}", self.fallback_height);
                    self.active_prepare_phase(self.fallback_height, qc, OPT).await?;
                }
                
                self.advance_height(self.height).await;

                // Make a new block if we are the next leader.
                if self.name == self.leader_elector.get_leader(self.height) {
                    self.opt_propose(self.height, None).await?;
                }
            }
        }
        Ok(())
    }

    /***********************pbft*************************/

    /***********************fallback**********************/

    async fn fallback_propose(&mut self, height: SeqNumber) -> ConsensusResult<()> {
        if self.prepare_tag.contains_key(&height) {
            return Ok(());
        }

        let block = self.generate_proposal(height, 1, None, None, PES).await;
        self.broadcast_fallback_propose(block).await
    }

    async fn broadcast_fallback_propose(&mut self, block: Block) -> ConsensusResult<()> {
        let message = ConsensusMessage::FBPropose(block.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;
        self.process_fallback_propose(&block).await?;
        if self.parameters.ddos {
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        }
        Ok(())
    }

    async fn make_fallback_vote(&mut self, block: &Block) -> Option<HVote> {
        Some(HVote::new(&block, block.round, self.name, PES, self.signature_service.clone()).await)
    }

    #[async_recursion]
    async fn handle_fallback_vote(&mut self, vote: &HVote) -> ConsensusResult<()> {
        if self.parameters.exp == 1 {
            vote.verify(&self.committee)?;
        }

        if let Some(qc) = self.aggregator.add_fallback_vote(vote.clone())? {
            self.fallback_high_qc
                .insert((qc.height, qc.round), Some(qc.clone()));
            if qc.proposer == self.name {
                if qc.round < self.fallback_length {
                    let block = self
                        .generate_proposal(qc.height, qc.round + 1, Some(qc.clone()), None, PES)
                        .await;
                    self.broadcast_fallback_propose(block).await?;
                } else if qc.round == self.fallback_length {
                    // No output from opt path til fallback proposals at qc.height finish.
                    // InvokeDBA(fallback_height, 1, \bot).
                    self.active_prepare_phase(qc.height, qc, PES).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_fallback_propose(&mut self, block: &Block) -> ConsensusResult<()> {
        if self.parameters.exp == 1 {
            block.verify(&self.committee)?
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone(), FALLBACK).await? {
            debug!(
                "Processing of {} suspended: missing payload",
                block.digest()
            );
            return Ok(());
        }

        self.process_fallback_propose(block).await?;
        Ok(())
    }

    async fn process_fallback_propose(&mut self, block: &Block) -> ConsensusResult<()> {
        self.store_block(block).await;

        if let Some(vote) = self.make_fallback_vote(block).await {
            if block.author != self.name {
                let message = ConsensusMessage::FBVote(vote);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&block.author),
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
            } else {
                self.handle_fallback_vote(&vote).await?;
            }
        }

        Ok(())
    }

    /***********************fallback**********************/

    /*************************Prepare**************************/

    async fn active_prepare_phase(
        &mut self,
        height: SeqNumber,
        proof: QC,
        val: u8,
    ) -> ConsensusResult<()> {
        if self.prepare_tag.contains_key(&height) {
            return Ok(());
        }

        let prepare = PrePare::new(
            self.name,
            height,
            proof,
            val,
            self.signature_service.clone(),
        )
        .await;

        self.prepare_tag.insert(height, true);

        let message = ConsensusMessage::ParPrePare(prepare.clone());

        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;

        self.handle_par_prepare(prepare).await?;

        Ok(())
    }

    async fn handle_par_prepare(&mut self, prepare: PrePare) -> ConsensusResult<()> {
        if prepare.height <= self.last_committed_fallback_height {
            debug!(
                "fallback prepare tag {} at fallback height: {} is outdated",
                prepare.val,
                prepare.height
            );
            return Ok(());
        }

        let opt_set = self
            .par_prepare_opts
            .entry(prepare.height)
            .or_insert(HashMap::new());
        let pes_set = self
            .par_prepare_pess
            .entry(prepare.height)
            .or_insert(HashMap::new());

        match prepare.val {
            OPT => {
                if opt_set.contains_key(&prepare.author) {
                    return Err(ConsensusError::AuthorityReuseinPrePare(prepare.author));
                }
                opt_set.insert(prepare.author, prepare.signature.clone());

                // DBA emits 0.
                if opt_set.len() as u32 == self.committee.quorum_threshold() {
                    if let Some(bytes) = self.store.read(prepare.proof.hash.to_vec()).await? 
                    {
                        // Fast commit the opt block.
                        let b0: Block = bincode::deserialize(&bytes)?;
                        self.commit(&b0).await?;
                        if let Err(e) = self.commit_channel.send(b0).await {
                            warn!("Failed to send block through the commit channel: {}", e);
                        }
                    }
                }

                // Invoke sMVBA with OPT.
                self.invoke_smvba(prepare.height, OPT,  Vec::new(), prepare.proof.clone()).await?;
            }
            PES => {
                if pes_set.contains_key(&prepare.author) {
                    return Err(ConsensusError::AuthorityReuseinPrePare(prepare.author));
                }
                pes_set.insert(prepare.author, prepare.signature);
                let signatures = pes_set
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if (pes_set.len() as u32) >= self.committee.quorum_threshold() {
                    //启动smvba
                    if let Some(qc) = self
                        .fallback_high_qc
                        .entry((prepare.height, self.fallback_length))
                        .or_insert(None)
                    {
                        let _qc = qc.clone();
                        self.invoke_smvba(prepare.height, PES, signatures, _qc)
                            .await?;
                    }
                }
            }
            _ => return Err(ConsensusError::InvalidPrePareTag(prepare.val)),
        }

        Ok(())
    }

    /*************************Prepare**************************/

    /******************SMVAB********************************************/
    async fn smvba_round_advance(
        &mut self,
        height: SeqNumber,
        round: SeqNumber,
    ) -> ConsensusResult<()> {
        info!(
            "-------------smvba round advance height {}, round {}------------",
            height, round
        );
        self.update_smvba_state(height, round);

        let proof = SPBProof {
            height,
            phase: INIT_PHASE,
            round,
            shares: Vec::new(),
        };

        if self.spb_proposes.contains_key(&(height, 1)) {
            let last_value = self.spb_proposes.get(&(height, 1)).unwrap().clone();

            let block = self
                .generate_proposal(
                    height,
                    self.fallback_length + 1,
                    Some(last_value.block.qc.clone()),
                    None,
                    PES,
                )
                .await;

            let value = SPBValue::new(
                block,
                round,
                INIT_PHASE,
                last_value.val,
                last_value.signatures.clone(),
            );

            self.broadcast_pes_propose(value, proof)
                .await
                .expect("Failed to send the PES block");
        }
        Ok(())
    }

    async fn invoke_smvba(
        &mut self,
        height: SeqNumber,
        val: u8,
        signatures: Vec<(PublicKey, Signature)>,
        proof: QC,
    ) -> ConsensusResult<()> {
        if *self.smvba_is_invoke.entry(height).or_insert(false) {
            return Ok(());
        }
        self.smvba_is_invoke.insert(height, true);

        let block;
        if val == OPT {
            // TODO[#1]: fill with the complete opt block.
            block = Block::opt(height, self.name);
        } else {
            block = self
                .generate_proposal(height, self.fallback_length + 1, Some(proof), None, PES)
                .await;
        }

        let round = self.smvba_current_round.entry(height).or_insert(1).clone();
        let value = SPBValue::new(block, round, INIT_PHASE, val, signatures);
        let proof = SPBProof {
            phase: INIT_PHASE,
            round,
            height,
            shares: Vec::new(),
        };
        self.broadcast_pes_propose(value, proof).await?;
        Ok(())
    }

    async fn broadcast_pes_propose(
        &mut self,
        value: SPBValue,
        proof: SPBProof,
    ) -> ConsensusResult<()> {
        if proof.phase == INIT_PHASE {
            self.spb_proposes
                .insert((value.block.height, value.round), value.clone());
        }

        let message = ConsensusMessage::SPBPropose(value.clone(), proof.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;

        self.process_spb_propose(&value, &proof).await?;

        // Wait for the minimum block delay.
        if self.parameters.ddos {
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        }

        Ok(())
    }

    //SMVBA only deal current round
    async fn handle_spb_proposal(
        &mut self,
        value: SPBValue,
        proof: SPBProof,
    ) -> ConsensusResult<()> {
        if proof.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA proposal at fallback height: {} round: {} is outdated",
                proof.height,
                proof.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            //验证Proof是否正确
            value.verify(&self.committee, &proof, self.fallback_length)?;
        }

        self.process_spb_propose(&value, &proof).await?;
        Ok(())
    }

    #[async_recursion]
    async fn process_spb_propose(
        &mut self,
        value: &SPBValue,
        proof: &SPBProof,
    ) -> ConsensusResult<()> {
        debug!("Processing PES Block {:?}", value.block);

        //如果是lock 阶段 保存
        if value.phase == LOCK_PHASE {
            self.spb_locks
                .entry((value.block.height, value.round))
                .or_insert(HashMap::new())
                .insert(value.block.author, (value.clone(), proof.clone()));
        }

        //vote
        if let Some(spb_vote) = self.make_spb_vote(&value).await {
            //将vote 广播给value 的 propose
            if self.name != value.block.author {
                let message = ConsensusMessage::SPBVote(spb_vote);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&value.block.author),
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
            } else {
                self.handle_spb_vote(&spb_vote).await?;
            }
        }
        Ok(())
    }

    async fn make_spb_vote(&mut self, value: &SPBValue) -> Option<SPBVote> {
        //有效性规则由其他过程完成
        if value.phase > LOCK_PHASE {
            return None;
        }
        Some(SPBVote::new(value.clone(), self.name, self.signature_service.clone()).await)
    }

    #[async_recursion]
    async fn handle_spb_vote(&mut self, spb_vote: &SPBVote) -> ConsensusResult<()> {
        if spb_vote.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA vote at fallback height: {} round: {} is outdated",
                spb_vote.height,
                spb_vote.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            spb_vote.verify(&self.committee)?;
        }
        if let Some(proof) = self.aggregator.add_spb_vote(spb_vote.clone())? {
            debug!("Create spb proof {:?}!", proof);

            let mut value = self
                .spb_proposes
                .get(&(proof.height, proof.round))
                .unwrap()
                .clone();
            //进行下一阶段的发送
            if proof.phase == LOCK_PHASE {
                value.phase = LOCK_PHASE;

                self.broadcast_pes_propose(value, proof).await?;
            } else if proof.phase == FIN_PHASE {
                value.phase = FIN_PHASE;

                let message = ConsensusMessage::SPBFinsh(value.clone(), proof.clone());

                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;

                self.handle_spb_finish(value, proof).await?;
            }
        }
        Ok(())
    }

    async fn handle_spb_finish(&mut self, value: SPBValue, proof: SPBProof) -> ConsensusResult<()> {
        if proof.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Finish at fallback height: {} round:{} is outdated",
                proof.height,
                proof.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            value.verify(&self.committee, &proof, self.fallback_length)?;
        }

        self.spb_finishs
            .entry((proof.height, proof.round))
            .or_insert(HashMap::new())
            .insert(value.block.author, (value.clone(), proof.clone()));

        let d_flag = self
            .smvba_d_flag
            .entry((proof.height, proof.round))
            .or_insert(false);

        if *d_flag {
            return Ok(());
        }

        let weight = self
            .spb_finishs
            .get(&(proof.height, proof.round))
            .unwrap()
            .len() as Stake;

        if weight == self.committee.quorum_threshold() {
            *d_flag = true;
            self.invoke_done_and_share(proof.height, proof.round)
                .await?;
        }

        Ok(())
    }

    async fn handle_smvba_done_with_share(&mut self, mdone: MDoneAndShare) -> ConsensusResult<()> {
        self.handle_smvba_done(&mdone).await?;
        self.handle_smvba_rs(&mdone.share).await?;
        Ok(())
    }

    #[async_recursion]
    async fn invoke_done_and_share(
        &mut self,
        height: SeqNumber,
        round: SeqNumber,
    ) -> ConsensusResult<()> {
        let share = RandomnessShare::new(
            height,
            round,
            self.name,
            self.signature_service.clone(),
        )
        .await;

        let mdone = MDoneAndShare::new(
            self.name,
            self.signature_service.clone(),
            height,
            round,
            share,
        )
        .await;

        let message = ConsensusMessage::SPBDoneAndShare(mdone.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;

        self.handle_smvba_done_with_share(mdone).await?;
        Ok(())
    }

    async fn handle_smvba_prevote(&mut self, prevote: MPreVote) -> ConsensusResult<()> {
        if prevote.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Prevote at fallback height: {} round: {} is outdated",
                prevote.height,
                prevote.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            prevote.verify(&self.committee, self.fallback_length)?;
        }

        let y_flag = self
            .smvba_y_flag
            .entry((prevote.height, prevote.round))
            .or_insert(false);
        let n_flag = self
            .smvba_n_flag
            .entry((prevote.height, prevote.round))
            .or_insert(false);

        let mut mvote: Option<MVote> = None;
        if !(*y_flag) && !(*n_flag) {
            match &prevote.tag {
                PreVoteTag::Yes(value, proof) => {
                    *y_flag = true;
                    if let Some(vote) = self.make_spb_vote(value).await {
                        mvote = Some(
                            MVote::new(
                                self.name,
                                prevote.leader,
                                self.signature_service.clone(),
                                prevote.round,
                                prevote.height,
                                MVoteTag::Yes(value.clone(), proof.clone(), vote),
                            )
                            .await,
                        );
                    }
                }
                PreVoteTag::No() => {
                    let set = self
                        .smvba_no_prevotes
                        .entry((prevote.height, prevote.round))
                        .or_insert(HashSet::new());
                    set.insert(prevote.author);
                    let weight = set.len() as Stake;

                    if weight == self.committee.quorum_threshold() {
                        *n_flag = true;
                        mvote = Some(
                            MVote::new(
                                self.name,
                                prevote.leader,
                                self.signature_service.clone(),
                                prevote.round,
                                prevote.height,
                                MVoteTag::No(),
                            )
                            .await,
                        );
                    }
                }
            }
        }

        if let Some(vote) = mvote {
            let message = ConsensusMessage::SMVBAVote(vote.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter_smvba,
                &self.committee,
                PES,
            )
            .await?;
            self.handle_smvba_mvote(vote).await?;
        }

        Ok(())
    }

    async fn handle_smvba_mvote(&mut self, mvote: MVote) -> ConsensusResult<()> {
        if mvote.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Vote at fallback height: {} round: {} is outdated",
                mvote.height,
                mvote.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            mvote.verify(&self.committee, &self.pk_set, self.fallback_length)?;
        }

        let set = self
            .smvba_votes
            .entry((mvote.height, mvote.round))
            .or_insert(HashSet::new());

        set.insert(mvote.author);

        let weight = set.len() as Stake;

        match mvote.tag {
            MVoteTag::Yes(value, _, vote) => {
                if let Some(fin_proof) = self.aggregator.add_pre_vote(vote)? {
                    let mhalt = MHalt::new(
                        self.name,
                        mvote.leader,
                        value,
                        fin_proof,
                        self.signature_service.clone(),
                    )
                    .await;

                    let message = ConsensusMessage::SMVBAHalt(mhalt.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter_smvba,
                        &self.committee,
                        PES,
                    )
                    .await?;
                    self.handle_smvba_halt(mhalt).await?;
                    return Ok(());
                }
            }
            MVoteTag::No() => {}
        };

        if weight == self.committee.quorum_threshold() {
            let current_round = self.smvba_current_round.get(&mvote.height).copied().unwrap_or(1);
            // Only advance if we're still in the round being voted on (haven't advanced yet)
            if current_round == mvote.round {
                self.smvba_round_advance(mvote.height, mvote.round + 1).await?;
            }
        }

        Ok(())
    }

    async fn handle_smvba_done(&mut self, mdone: &MDoneAndShare) -> ConsensusResult<()> {
        if mdone.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Done at fallback height: {} round: {} is outdated",
                mdone.height,
                mdone.round
            );
            return Ok(());
        }
        
        if self.parameters.exp == 1 {
            mdone.verify(&self.committee, &self.pk_set)?;
        }

        let d_flag = self
            .smvba_d_flag
            .entry((mdone.height, mdone.round))
            .or_insert(false);

        let set = self
            .smvba_dones
            .entry((mdone.height, mdone.round))
            .or_insert(HashSet::new());
        set.insert(mdone.author);
        let weight = set.len() as Stake;

        // d_flag= false and weight == f+1?
        if *d_flag == false && weight == self.committee.random_coin_threshold() {
            *d_flag = true;
            // set.insert(self.name);
            // weight += 1;
            self.invoke_done_and_share(mdone.height, mdone.round)
                .await?;
            return Ok(());
        }

        // 2f+1?
        if weight == self.committee.quorum_threshold() {
            //abandon spb message
            self.spb_abandon_flag.insert(mdone.height, true);
        }

        Ok(())
    }

    async fn handle_smvba_rs(&mut self, share: &RandomnessShare) -> ConsensusResult<()> {
        if share.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Share at fallback height: {} round: {} is outdated",
                share.height,
                share.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            share.verify(&self.committee, &self.pk_set)?;
        }

        if self
            .leader_elector
            .get_coin_leader(share.height, share.round)
            .is_some()
        {
            return Ok(());
        }
        let height = share.height;
        let round = share.round;

        if let Some(coin) = self
            .aggregator
            .add_smvba_random(share.clone(), &self.pk_set)?
        {
            debug!("Coin Leader {:?}", coin);
            self.leader_elector.add_random_coin(coin.clone());

            let leader = coin.leader;

            // container finish?
            if self
                .spb_finishs
                .entry((coin.height, coin.round))
                .or_insert(HashMap::new())
                .contains_key(&leader)
            {
                let (value, proof) = self
                    .spb_finishs
                    .get(&(coin.height, coin.round))
                    .unwrap()
                    .get(&leader)
                    .unwrap();
                let mhalt = MHalt::new(
                    self.name,
                    leader,
                    value.clone(),
                    proof.clone(),
                    self.signature_service.clone(),
                )
                .await;

                let message = ConsensusMessage::SMVBAHalt(mhalt.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_smvba_halt(mhalt).await?;
            } else {
                let mut pre_vote = MPreVote::new(
                    self.name,
                    leader,
                    self.signature_service.clone(),
                    round,
                    height,
                    PreVoteTag::No(),
                )
                .await;

                //container lock?
                if self
                    .spb_locks
                    .entry((coin.height, coin.round))
                    .or_insert(HashMap::new())
                    .contains_key(&leader)
                {
                    let (value, proof) = self
                        .spb_locks
                        .get(&(coin.height, coin.round))
                        .unwrap()
                        .get(&leader)
                        .unwrap();
                    pre_vote = MPreVote::new(
                        self.name,
                        leader,
                        self.signature_service.clone(),
                        round,
                        height,
                        PreVoteTag::Yes(value.clone(), proof.clone()),
                    )
                    .await;
                }
                let message = ConsensusMessage::SMVBAPreVote(pre_vote.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_smvba_prevote(pre_vote).await?;
            }
        }

        Ok(())
    }

    async fn handle_smvba_halt(&mut self, halt: MHalt) -> ConsensusResult<()> {
        if halt.height <= self.last_committed_fallback_height {
            debug!(
                "sMVBA Halt at fallback height: {} round: {} is outdated",
                halt.height,
                halt.round
            );
            return Ok(());
        }

        if self.parameters.exp == 1 {
            halt.verify(&self.committee, &self.pk_set, self.fallback_length)?;
        }

        if self.leader_elector.get_coin_leader(halt.height, halt.round)
            != Some(halt.value.block.author)
        // leader 是否与 finish value的proposer 相符
        {
            return Ok(());
        }

        // halt?
        if *self.smvba_halt_falg.entry(halt.height).or_insert(false) {
            return Ok(());
        }

        self.smvba_halt_falg.insert(halt.height, true);

        if halt.value.val == OPT {
            // Try to advance fallback height.
            self.advance_fallback_height(halt.height).await?;
            return Ok(())
        }

        let block = halt.value.block;
        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone(), PES).await? {
            debug!(
                "Processing of {} suspended: missing payload",
                block.digest()
            );
            return Ok(());
        }

        self.process_par_out(&block).await?;

        Ok(())
    }

    async fn process_par_out(&mut self, block: &Block) -> ConsensusResult<()> {
        // Try to advance fallback height.
        self.advance_fallback_height(block.height).await?;

        self.store_block(block).await;

        debug!("sMVBA Committed {:?}", block);
        self.commit(block).await?;
        if let Err(e) = self.commit_channel.send(block.clone()).await {
            warn!("Failed to send block through the commit channel: {}", e);
        }

        self.mempool_driver.cleanup_par(block).await;

        Ok(())
    }

    async fn advance_fallback_height(&mut self, height: SeqNumber) -> ConsensusResult<()> {
        if self.fallback_height <= height {
            self.fallback_height = height + 1;
            debug!("Moved to fallback height: {} after last DBA ended", self.fallback_height);
        }

        // Start fallback proposals for next DBA.
        self.fallback_propose(height + 1).await
    }

    /******************SMVAB**************************************************************/

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        if self.opt_path {
            self.timer.reset();

            // Upon booting, generate the very first block (if we are the leader).
            if self.name == self.leader_elector.get_leader(1) {
                self.opt_propose(1, None).await.expect("Failed to send the first OPT block");
            }
        }

        if self.pes_path {
            self.fallback_propose(1).await.expect("Failed to send the first PES block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        ConsensusMessage::HsPropose(block) => self.handle_opt_proposal(&block).await,
                        ConsensusMessage::HSVote(vote) => self.handle_opt_vote(&vote).await,
                        ConsensusMessage::HsTimeout(timeout) => self.handle_timeout(&timeout).await,
                        ConsensusMessage::HsLoopBack(block) => self.process_opt_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await,
                        ConsensusMessage::SyncReply(block) => self.handle_opt_proposal(&block).await,
                        _=> Ok(()),
                    }
                },
                Some(message) = self.smvba_channel.recv() => {
                    match message {
                        ConsensusMessage::FBPropose(block)=>self.handle_fallback_propose(&block).await,
                        ConsensusMessage::FBVote(vote)=>self.handle_fallback_vote(&vote).await,
                        ConsensusMessage::FBLoopBack(block)=>self.process_fallback_propose(&block).await,
                        ConsensusMessage::SPBPropose(value,proof)=> self.handle_spb_proposal(value,proof).await,
                        ConsensusMessage::SPBVote(vote)=> self.handle_spb_vote(&vote).await,
                        ConsensusMessage::SPBFinsh(value,proof)=> self.handle_spb_finish(value,proof).await,
                        ConsensusMessage::SPBDoneAndShare(done) => self.handle_smvba_done_with_share(done).await,
                        ConsensusMessage::SMVBAPreVote(prevote) => self.handle_smvba_prevote(prevote).await,
                        ConsensusMessage::SMVBAVote(mvote) => self.handle_smvba_mvote(mvote).await,
                        ConsensusMessage::SMVBAHalt(halt) => self.handle_smvba_halt(halt).await,
                        ConsensusMessage::ParPrePare(prepare) => self.handle_par_prepare(prepare).await,
                        ConsensusMessage::ParLoopBack(block) => self.process_par_out(&block).await,
                        _=> Ok(()),
                    }
                },
                () = &mut self.timer => self.local_timeout_round().await,
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => {
                    warn!("{}", e)
                }
            }
        }
    }
}
