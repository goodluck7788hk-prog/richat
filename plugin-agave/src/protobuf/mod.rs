mod alpenglow;
mod encoding;
mod message;

pub use {
    encoding::{Account, BlockMeta, Entry, Slot, Transaction, bytes_encode, bytes_encoded_len},
    message::{ProtobufEncoder, ProtobufMessage},
};

#[cfg(test)]
mod tests {
    use {
        super::{ProtobufEncoder, ProtobufMessage},
        prost::Message,
        richat_benches::fixtures::{
            BANK_ID, generate_accounts, generate_block_metas, generate_entries, generate_slots,
            generate_transactions,
        },
        richat_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
        std::time::SystemTime,
    };

    #[test]
    pub fn test_encode_account() {
        let created_at = SystemTime::now();
        for item in generate_accounts() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Account {
                bank_id: Some(BANK_ID),
                slot,
                account: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "account: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Account(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "account: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_block_meta() {
        let created_at = SystemTime::now();
        for item in generate_block_metas() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::BlockMeta {
                bank_id: BANK_ID,
                blockinfo: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "block meta: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::BlockMeta(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "block meta: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_entry() {
        let created_at = SystemTime::now();
        for item in generate_entries() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::Entry {
                bank_id: BANK_ID,
                entry: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "entry: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Entry(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "entry: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_slot() {
        let created_at = SystemTime::now();
        for item in generate_slots() {
            let (slot, parent, status) = item.to_replica();
            let msg_richat = ProtobufMessage::Slot {
                bank_id: Some(BANK_ID),
                slot,
                parent,
                status,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "slot: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Slot(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "slot: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_transaction() {
        let created_at = SystemTime::now();
        for item in generate_transactions() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Transaction {
                bank_id: BANK_ID,
                slot,
                transaction: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "transaction: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Transaction(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "transaction: {item:?}");
        }
    }

    fn encode_block_footer(
        cert: &solana_entry::block_component::BlockFinalizationCert,
    ) -> richat_proto::geyser::SubscribeUpdateBlockFooter {
        use {
            agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockFooterInfo,
            solana_entry::block_component::{BlockFooterV1, VersionedBlockFooter},
            solana_hash::Hash,
        };

        let created_at = SystemTime::now();
        let block_footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_from_array([7; 32]),
            block_producer_time_nanos: 99,
            block_user_agent: b"agent".to_vec(),
            block_final_cert: Some(cert.clone()),
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        let replica = ReplicaBlockFooterInfo {
            slot: cert.slot,
            block_footer: &block_footer,
        };
        let msg = ProtobufMessage::BlockFooter {
            footer: &replica,
            bank_id: BANK_ID,
        };
        let vec_prost = msg.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
        let vec_raw = msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
        assert_eq!(vec_prost, vec_raw);

        let Some(UpdateOneof::BlockFooter(footer)) = SubscribeUpdate::decode(vec_raw.as_slice())
            .unwrap()
            .update_oneof
        else {
            panic!("expected block footer");
        };
        assert_eq!(footer.slot, cert.slot);
        assert_eq!(footer.bank_id, BANK_ID);
        assert_eq!(footer.bank_hash, vec![7; 32]);
        assert_eq!(footer.block_producer_time_nanos, 99);
        assert_eq!(footer.block_user_agent, b"agent");
        assert!(footer.skip_reward_cert.is_none() && footer.notar_reward_cert.is_none());
        footer
    }

    #[test]
    pub fn test_encode_block_footer_slow_finalization() {
        use {
            agave_votor_messages::certificate::CertSignature,
            solana_bls_signatures::{Keypair, Signature, SignatureCompressed},
            solana_entry::block_component::{BlockFinalizationCert, VotesAggregate},
            solana_hash::Hash,
        };

        let keypair = Keypair::new();
        let final_signature = Signature::from(keypair.sign(b"finalize"));
        let notar_signature = Signature::from(keypair.sign(b"notarize"));
        let cert = BlockFinalizationCert {
            slot: 10,
            block_id: Hash::new_from_array([1; 32]),
            final_aggregate: VotesAggregate::from_cert_signature(CertSignature {
                signature: final_signature,
                bitmap: vec![1, 2, 3],
            }),
            notar_aggregate: Some(VotesAggregate::from_cert_signature(CertSignature {
                signature: notar_signature,
                bitmap: vec![4, 5],
            })),
        };

        let final_cert = encode_block_footer(&cert).block_final_cert.unwrap();
        assert_eq!(final_cert.slot, 10);

        // slow finalization: final aggregate votes on the slot, notar aggregate on the block
        let final_aggregate = final_cert.final_aggregate.unwrap();
        assert_eq!(
            final_aggregate.signature,
            SignatureCompressed::try_from(&final_signature).unwrap().0
        );
        assert_eq!(final_aggregate.signer_bitmap, vec![1, 2, 3]);
        assert!(final_aggregate.block_id.is_empty());

        let notar_aggregate = final_cert.notar_aggregate.unwrap();
        assert_eq!(
            notar_aggregate.signature,
            SignatureCompressed::try_from(&notar_signature).unwrap().0
        );
        assert_eq!(notar_aggregate.signer_bitmap, vec![4, 5]);
        assert_eq!(notar_aggregate.block_id, vec![1; 32]);
    }

    #[test]
    pub fn test_encode_block_footer_invalid_cert_dropped() {
        // all-zero signature is not a valid BLS point: the cert is dropped, the footer still sent
        let cert = solana_entry::block_component::BlockFinalizationCert::new_for_tests();
        assert!(encode_block_footer(&cert).block_final_cert.is_none());
    }

    #[test]
    pub fn test_encode_entry_update_parent() {
        use {
            agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryUpdateParentInfo,
            solana_hash::Hash,
        };

        let created_at = SystemTime::now();
        let parent_block_id = Hash::new_from_array([9; 32]);
        let replica = ReplicaEntryUpdateParentInfo {
            slot: 10,
            cleared_bank_id: BANK_ID,
            parent_slot: 9,
            parent_block_id: &parent_block_id,
        };
        let msg = ProtobufMessage::EntryUpdateParent {
            update_parent: &replica,
        };
        let vec_prost = msg.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
        let vec_raw = msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
        assert_eq!(vec_prost, vec_raw);

        let Some(UpdateOneof::EntryUpdateParent(update)) =
            SubscribeUpdate::decode(vec_raw.as_slice())
                .unwrap()
                .update_oneof
        else {
            panic!("expected entry update parent");
        };
        assert_eq!(
            (update.slot, update.cleared_bank_id, update.parent_slot),
            (10, BANK_ID, 9)
        );
        assert_eq!(update.parent_block_id, vec![9; 32]);
    }
}
