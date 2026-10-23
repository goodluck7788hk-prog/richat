use {
    crate::{channel::ParsedMessage, metrics},
    ::metrics::gauge,
    foldhash::quality::RandomState,
    futures::future::TryFutureExt,
    richat_proto::geyser::{CommitmentLevel as CommitmentLevelProto, SlotStatus},
    solana_clock::{MAX_PROCESSING_AGE, Slot},
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        sync::Arc,
    },
    tokio::sync::oneshot,
    tonic::Status,
};

#[derive(Debug, Default, Clone)]
pub struct BlockMeta {
    pub slot: Slot,
    pub blockhash: Arc<String>,
    pub block_height: Slot,

    bank_id: u64,
}

#[derive(Debug, Default)]
struct SlotCandidates {
    candidates: Vec<BlockMeta>,
    selected_bank_id: Option<u64>,
    cleared_banks: HashSet<u64, RandomState>,
    confirmed: bool,
    finalized: bool,
}

impl SlotCandidates {
    fn selected(&self) -> Option<&BlockMeta> {
        if (self.confirmed || self.finalized) && self.selected_bank_id.is_none() {
            return None;
        }
        self.candidates.iter().rev().find(|candidate| {
            self.selected_bank_id
                .is_none_or(|bank_id| candidate.bank_id == bank_id)
        })
    }
}

#[derive(Debug, Default)]
struct BlockStatus {
    last_valid_block_height: Slot,

    processed: bool, // flag, means that we received block meta message
    confirmed: bool,
    finalized: bool,
}

#[derive(Debug, Clone)]
pub struct BlockMetaStorage {
    messages_tx: kanal::AsyncSender<ParsedMessage>,
    requests_tx: kanal::AsyncSender<Request>,
}

impl BlockMetaStorage {
    pub fn new(request_queue_size: usize) -> (Self, impl Future<Output = anyhow::Result<()>>) {
        let (messages_tx, messages_rx) = kanal::unbounded_async();
        let (requests_tx, requests_rx) = kanal::bounded_async(request_queue_size);

        let me = Self {
            messages_tx,
            requests_tx,
        };
        let fut = tokio::spawn(Self::work(messages_rx, requests_rx)).map_err(anyhow::Error::new);

        (me, fut)
    }

    async fn work(
        messages_rx: kanal::AsyncReceiver<ParsedMessage>,
        requests_rx: kanal::AsyncReceiver<Request>,
    ) {
        let mut state = State::default();
        let messages = messages_rx.recv();
        let requests = requests_rx.recv();
        tokio::pin!(messages, requests);
        loop {
            tokio::select! {
                biased;
                message = &mut messages => {
                    messages.set(messages_rx.recv());
                    match message {
                        Ok(message) => state.handle_message(message),
                        Err(_) => break,
                    }
                },
                request = &mut requests => {
                    requests.set(requests_rx.recv());
                    gauge!(metrics::GRPC_BLOCK_META_QUEUE_SIZE).decrement(1);
                    match request {
                        Ok(Request::GetBlock(tx, commitment)) => {
                            let _ = tx.send(state.get_block(commitment));
                        }
                        Ok(Request::IsBlockhashValid(tx, blockhash, commitment)) => {
                            let _ = tx.send(state.is_blockhash_valid(&blockhash, commitment));
                        }
                        Err(_) => break,
                    }
                }
            };
        }
    }

    pub fn push(&self, message: ParsedMessage) {
        let _ = self.messages_tx.try_send(message);
    }

    async fn send_request<T>(
        &self,
        request: Request,
        rx: oneshot::Receiver<Option<T>>,
    ) -> tonic::Result<T> {
        if self.requests_tx.try_send(request).is_err() {
            return Err(tonic::Status::resource_exhausted("queue channel is full"));
        }

        gauge!(metrics::GRPC_BLOCK_META_QUEUE_SIZE).increment(1);
        match rx.await {
            Ok(Some(block)) => Ok(block),
            Ok(None) => Err(Status::aborted("failed to get result")),
            Err(_) => Err(Status::aborted("failed to wait response")),
        }
    }

    pub async fn get_block(&self, commitment: CommitmentLevelProto) -> tonic::Result<BlockMeta> {
        let (tx, rx) = oneshot::channel();
        let request = Request::GetBlock(tx, commitment);
        self.send_request(request, rx).await
    }

    pub async fn is_blockhash_valid(
        &self,
        blockhash: String,
        commitment: CommitmentLevelProto,
    ) -> tonic::Result<(bool, Slot)> {
        let (tx, rx) = oneshot::channel();
        let request = Request::IsBlockhashValid(tx, blockhash, commitment);
        self.send_request(request, rx).await
    }
}

enum Request {
    GetBlock(oneshot::Sender<Option<BlockMeta>>, CommitmentLevelProto),
    IsBlockhashValid(
        oneshot::Sender<Option<(bool, Slot)>>,
        String,
        CommitmentLevelProto,
    ),
}

#[derive(Debug, Default)]
struct State {
    blocks: HashMap<Slot, SlotCandidates, RandomState>,
    blockhashes: HashMap<Arc<String>, BlockStatus, RandomState>,
    processed: Slot,
    confirmed: Slot,
    finalized: Slot,
}

impl State {
    fn handle_message(&mut self, message: ParsedMessage) {
        let slot = message.slot();
        if slot < self.finalized {
            return;
        }
        match message {
            ParsedMessage::Slot(message) => {
                let status = message.status();
                if !matches!(
                    status,
                    SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized
                ) {
                    return;
                }
                let entry = self.blocks.entry(slot).or_default();
                if status == SlotStatus::SlotFinalized
                    || !entry.confirmed
                    || (entry.selected_bank_id.is_none() && message.bank_id().is_some())
                {
                    entry.selected_bank_id =
                        message.bank_id().or(entry.selected_bank_id).or_else(|| {
                            (entry.candidates.len() == 1).then(|| entry.candidates[0].bank_id)
                        });
                }
                if status == SlotStatus::SlotConfirmed {
                    entry.confirmed = true;
                    self.confirmed = self.confirmed.max(slot);
                    gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "confirmed")
                        .set(self.confirmed as f64);
                } else {
                    entry.confirmed = true;
                    entry.finalized = true;
                    self.finalized = self.finalized.max(slot);
                    self.confirmed = self.confirmed.max(slot);
                    gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "finalized")
                        .set(self.finalized as f64);
                }
                self.update_hash_statuses(slot);
                self.prune();
            }
            ParsedMessage::BlockMeta(message) => {
                let bank_id = message.bank_id();
                let entry = self.blocks.entry(slot).or_default();
                if entry.cleared_banks.contains(&bank_id)
                    || (entry.finalized
                        && entry
                            .selected_bank_id
                            .is_some_and(|selected| selected != bank_id))
                {
                    return;
                }
                let candidate = BlockMeta {
                    slot,
                    bank_id,
                    blockhash: Arc::new(message.blockhash().to_owned()),
                    block_height: message.block_height(),
                };
                self.blockhashes
                    .entry(Arc::clone(&candidate.blockhash))
                    .or_default()
                    .last_valid_block_height = candidate.block_height + MAX_PROCESSING_AGE as u64;
                if let Some(previous) = entry
                    .candidates
                    .iter_mut()
                    .find(|previous| previous.bank_id == bank_id)
                {
                    *previous = candidate;
                } else {
                    entry.candidates.push(candidate);
                }
                if (entry.confirmed || entry.finalized)
                    && entry.selected_bank_id.is_none()
                    && entry.candidates.len() == 1
                {
                    entry.selected_bank_id = Some(bank_id);
                }
                self.processed = self.processed.max(slot);
                gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "processed")
                    .set(self.processed as f64);
                self.update_hash_statuses(slot);
                self.prune();
            }
            ParsedMessage::EntryUpdateParent(message) => {
                let bank_id = message.cleared_bank_id();
                let entry = self.blocks.entry(slot).or_default();
                if (entry.confirmed || entry.finalized) && entry.selected_bank_id == Some(bank_id) {
                    return;
                }
                entry.cleared_banks.insert(bank_id);
                for candidate in entry
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.bank_id == bank_id)
                {
                    if let Some(status) = self.blockhashes.get_mut(&candidate.blockhash) {
                        status.processed = false;
                        status.confirmed = false;
                        status.finalized = false;
                    }
                }
                entry
                    .candidates
                    .retain(|candidate| candidate.bank_id != bank_id);
                self.update_hash_statuses(slot);
            }
            _ => {}
        }
    }

    fn update_hash_statuses(&mut self, slot: Slot) {
        let entry = &self.blocks[&slot];
        for candidate in &entry.candidates {
            if let Some(status) = self.blockhashes.get_mut(&candidate.blockhash) {
                status.processed = false;
                status.confirmed = false;
                status.finalized = false;
            }
        }
        for candidate in entry.candidates.iter().filter(|candidate| {
            entry
                .selected_bank_id
                .is_none_or(|selected| selected == candidate.bank_id)
        }) {
            if let Some(status) = self.blockhashes.get_mut(&candidate.blockhash) {
                status.processed = true;
                status.confirmed = entry.confirmed && entry.selected_bank_id.is_some();
                status.finalized = entry.finalized && entry.selected_bank_id.is_some();
            }
        }
    }

    fn prune(&mut self) {
        if let Some(block) = self
            .blocks
            .get(&self.finalized)
            .and_then(SlotCandidates::selected)
        {
            let block_height = block.block_height;
            self.blockhashes
                .retain(|_, status| status.last_valid_block_height >= block_height);
            self.blocks.retain(|slot, _| *slot >= self.finalized);
        }
    }

    fn get_block(&self, commitment: CommitmentLevelProto) -> Option<BlockMeta> {
        let slot = match commitment {
            CommitmentLevelProto::Processed => self.processed,
            CommitmentLevelProto::Confirmed => self.confirmed,
            CommitmentLevelProto::Finalized => self.finalized,
        };
        self.blocks.get(&slot)?.selected().cloned()
    }

    fn is_blockhash_valid(
        &self,
        blockhash: &str,
        commitment: CommitmentLevelProto,
    ) -> Option<(bool, Slot)> {
        let block = self.get_block(commitment)?;
        let valid = self
            .blockhashes
            .get(&blockhash.to_owned())
            .is_some_and(|status| {
                let accepted = match commitment {
                    CommitmentLevelProto::Processed => status.processed,
                    CommitmentLevelProto::Confirmed => status.confirmed,
                    CommitmentLevelProto::Finalized => status.finalized,
                };
                accepted && block.block_height <= status.last_valid_block_height
            });
        Some((valid, block.slot))
    }
}

#[cfg(test)]
mod stability_tests;
