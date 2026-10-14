use {
    crate::protobuf::{alpenglow, encoding},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockFooterInfo, ReplicaBlockInfoV4, ReplicaEntryInfoV2,
        ReplicaEntryUpdateParentInfo, ReplicaTransactionInfoV3, SlotStatus as GeyserSlotStatus,
    },
    prost::encoding::message,
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    std::time::SystemTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtobufEncoder {
    Prost,
    Raw,
}

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account {
        slot: Slot,
        account: &'a ReplicaAccountInfoV3<'a>,
        bank_id: Option<BankId>,
    },
    Slot {
        slot: Slot,
        parent: Option<u64>,
        status: &'a GeyserSlotStatus,
        bank_id: Option<BankId>,
    },
    Transaction {
        slot: Slot,
        transaction: &'a ReplicaTransactionInfoV3<'a>,
        bank_id: BankId,
    },
    Entry {
        entry: &'a ReplicaEntryInfoV2<'a>,
        bank_id: BankId,
    },
    BlockMeta {
        blockinfo: &'a ReplicaBlockInfoV4<'a>,
        bank_id: BankId,
    },
    BlockFooter {
        footer: &'a ReplicaBlockFooterInfo<'a>,
        bank_id: BankId,
    },
    EntryUpdateParent {
        update_parent: &'a ReplicaEntryUpdateParentInfo<'a>,
    },
}

impl ProtobufMessage<'_> {
    pub const fn get_slot(&self) -> Slot {
        match self {
            Self::Account { slot, .. } => *slot,
            Self::Slot { slot, .. } => *slot,
            Self::Transaction { slot, .. } => *slot,
            Self::Entry { entry, .. } => entry.slot,
            Self::BlockMeta { blockinfo, .. } => blockinfo.slot,
            Self::BlockFooter { footer, .. } => footer.slot,
            Self::EntryUpdateParent { update_parent } => update_parent.slot,
        }
    }

    pub fn encode(&self, encoder: ProtobufEncoder) -> Vec<u8> {
        self.encode_with_timestamp(encoder, SystemTime::now())
    }

    pub fn encode_with_timestamp(
        &self,
        encoder: ProtobufEncoder,
        created_at: impl Into<Timestamp>,
    ) -> Vec<u8> {
        match encoder {
            ProtobufEncoder::Prost => self.encode_prost(created_at),
            ProtobufEncoder::Raw => self.encode_raw(created_at),
        }
    }

    pub fn encode_prost(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        use {
            prost::Message,
            richat_proto::{
                convert_to,
                geyser::{
                    SlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
                    SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
                    SubscribeUpdateSlot, SubscribeUpdateTransaction,
                    SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
                },
            },
        };

        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(match self {
                Self::Account {
                    slot,
                    account,
                    bank_id,
                } => UpdateOneof::Account(SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: account.pubkey.to_vec(),
                        lamports: account.lamports,
                        owner: account.owner.to_vec(),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.to_vec(),
                        write_version: account.write_version,
                        txn_signature: account
                            .txn
                            .as_ref()
                            .map(|transaction| transaction.signature().as_ref().to_vec()),
                    }),
                    slot: *slot,
                    is_startup: false,
                    bank_id: *bank_id,
                }),
                Self::Slot {
                    slot,
                    parent,
                    status,
                    bank_id,
                } => UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: *slot,
                    parent: *parent,
                    status: match status {
                        GeyserSlotStatus::Processed => SlotStatus::SlotProcessed,
                        GeyserSlotStatus::Rooted => SlotStatus::SlotFinalized,
                        GeyserSlotStatus::Confirmed => SlotStatus::SlotConfirmed,
                        GeyserSlotStatus::FirstShredReceived => SlotStatus::SlotFirstShredReceived,
                        GeyserSlotStatus::Completed => SlotStatus::SlotCompleted,
                        GeyserSlotStatus::CreatedBank => SlotStatus::SlotCreatedBank,
                        GeyserSlotStatus::Dead(_) => SlotStatus::SlotDead,
                    } as i32,
                    dead_error: if let GeyserSlotStatus::Dead(error) = status {
                        Some(error.clone())
                    } else {
                        None
                    },
                    bank_id: *bank_id,
                }),
                Self::Transaction {
                    slot,
                    transaction,
                    bank_id,
                } => UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: transaction.signature.as_ref().to_vec(),
                        is_vote: transaction.is_vote,
                        transaction: Some(convert_to::create_transaction(transaction.transaction)),
                        meta: Some(convert_to::create_transaction_meta(
                            transaction.transaction_status_meta,
                        )),
                        index: transaction.index as u64,
                    }),
                    slot: *slot,
                    bank_id: *bank_id,
                }),
                Self::BlockMeta { blockinfo, bank_id } => {
                    UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: blockinfo.slot,
                        blockhash: blockinfo.blockhash.to_string(),
                        rewards: Some(convert_to::create_rewards_obj(
                            &blockinfo.rewards.rewards,
                            blockinfo.rewards.num_partitions,
                        )),
                        block_time: blockinfo.block_time.map(convert_to::create_timestamp),
                        block_height: blockinfo.block_height.map(convert_to::create_block_height),
                        parent_slot: blockinfo.parent_slot,
                        parent_blockhash: blockinfo.parent_blockhash.to_string(),
                        executed_transaction_count: blockinfo.executed_transaction_count,
                        entries_count: blockinfo.entry_count,
                        bank_id: *bank_id,
                    })
                }
                Self::Entry { entry, bank_id } => UpdateOneof::Entry(SubscribeUpdateEntry {
                    slot: entry.slot,
                    index: entry.index as u64,
                    num_hashes: entry.num_hashes,
                    hash: entry.hash.to_vec(),
                    executed_transaction_count: entry.executed_transaction_count,
                    starting_transaction_index: entry.starting_transaction_index as u64,
                    bank_id: *bank_id,
                }),
                Self::BlockFooter { footer, bank_id } => {
                    UpdateOneof::BlockFooter(alpenglow::create_block_footer(footer, *bank_id))
                }
                Self::EntryUpdateParent { update_parent } => UpdateOneof::EntryUpdateParent(
                    alpenglow::create_entry_update_parent(update_parent),
                ),
            }),
            created_at: Some(created_at.into()),
        }
        .encode_to_vec()
    }

    pub fn encode_raw(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        let created_at = created_at.into();

        let size = match self {
            Self::Account {
                slot,
                account,
                bank_id,
            } => {
                let account = encoding::Account::new(*slot, account, *bank_id);
                message::encoded_len(2, &account)
            }
            Self::Slot {
                slot,
                parent,
                status,
                bank_id,
            } => {
                let slot = encoding::Slot::new(*slot, *parent, status, *bank_id);
                message::encoded_len(3, &slot)
            }
            Self::Transaction {
                slot,
                transaction,
                bank_id,
            } => {
                let transaction = encoding::Transaction::new(*slot, transaction, *bank_id);
                message::encoded_len(4, &transaction)
            }
            Self::BlockMeta { blockinfo, bank_id } => {
                let blockmeta = encoding::BlockMeta::new(blockinfo, *bank_id);
                message::encoded_len(7, &blockmeta)
            }
            Self::Entry { entry, bank_id } => {
                let entry = encoding::Entry::new(entry, *bank_id);
                message::encoded_len(8, &entry)
            }
            Self::BlockFooter { footer, bank_id } => {
                message::encoded_len(12, &alpenglow::create_block_footer(footer, *bank_id))
            }
            Self::EntryUpdateParent { update_parent } => {
                message::encoded_len(13, &alpenglow::create_entry_update_parent(update_parent))
            }
        } + message::encoded_len(11, &created_at);

        let mut vec = Vec::with_capacity(size);
        let buffer = &mut vec;

        match self {
            Self::Account {
                slot,
                account,
                bank_id,
            } => {
                let account = encoding::Account::new(*slot, account, *bank_id);
                message::encode(2, &account, buffer)
            }
            Self::Slot {
                slot,
                parent,
                status,
                bank_id,
            } => {
                let slot = encoding::Slot::new(*slot, *parent, status, *bank_id);
                message::encode(3, &slot, buffer)
            }
            Self::Transaction {
                slot,
                transaction,
                bank_id,
            } => {
                let transaction = encoding::Transaction::new(*slot, transaction, *bank_id);
                message::encode(4, &transaction, buffer)
            }
            Self::BlockMeta { blockinfo, bank_id } => {
                let blockmeta = encoding::BlockMeta::new(blockinfo, *bank_id);
                message::encode(7, &blockmeta, buffer)
            }
            Self::Entry { entry, bank_id } => {
                let entry = encoding::Entry::new(entry, *bank_id);
                message::encode(8, &entry, buffer)
            }
            Self::BlockFooter { footer, bank_id } => message::encode(
                12,
                &alpenglow::create_block_footer(footer, *bank_id),
                buffer,
            ),
            Self::EntryUpdateParent { update_parent } => message::encode(
                13,
                &alpenglow::create_entry_update_parent(update_parent),
                buffer,
            ),
        }
        message::encode(11, &created_at, buffer);

        vec
    }
}
