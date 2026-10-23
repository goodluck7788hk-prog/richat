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
        let cert = solana_entry::block_component::BlockFinalizationCert::new_for_tests();
        assert!(encode_block_footer(&cert).block_final_cert.is_none());
    }

    #[test]
    fn test_encode_block_footer_fast_finalization() {
        use {
            agave_votor_messages::certificate::CertSignature,
            solana_bls_signatures::{Keypair, Signature, SignatureCompressed},
            solana_entry::block_component::{BlockFinalizationCert, VotesAggregate},
            solana_hash::Hash,
        };

        let signature = Signature::from(Keypair::new().sign(b"fast finalize"));
        let cert = BlockFinalizationCert {
            slot: 10,
            block_id: Hash::new_from_array([2; 32]),
            final_aggregate: VotesAggregate::from_cert_signature(CertSignature {
                signature,
                bitmap: vec![6, 7],
            }),
            notar_aggregate: None,
        };
        let final_cert = encode_block_footer(&cert).block_final_cert.unwrap();
        assert!(final_cert.notar_aggregate.is_none());
        let aggregate = final_cert.final_aggregate.unwrap();
        assert_eq!(aggregate.signature_kind, 0);
        assert_eq!(
            aggregate.signature,
            SignatureCompressed::try_from(&signature).unwrap().0
        );
        assert_eq!(aggregate.block_id, vec![2; 32]);
        assert_eq!(aggregate.signer_bitmap, vec![6, 7]);
    }

    #[test]
    fn test_encode_block_footer_reward_certificates_survive_invalid_final_cert() {
        use {
            agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockFooterInfo,
            agave_votor_messages::{
                certificate::CertSignature,
                reward_certificate::{NotarRewardCertificate, SkipRewardCertificate},
            },
            solana_bls_signatures::{Keypair, Signature, SignatureCompressed},
            solana_entry::block_component::{
                BlockFinalizationCert, BlockFooterV1, VersionedBlockFooter, VotesAggregate,
            },
            solana_hash::Hash,
        };

        let signature = Signature::from(Keypair::new().sign(b"reward"));
        let compressed = SignatureCompressed::try_from(&signature).unwrap();
        let invalid = BlockFinalizationCert::new_for_tests();
        let cert = BlockFinalizationCert {
            slot: 10,
            block_id: Hash::new_from_array([2; 32]),
            final_aggregate: VotesAggregate::from_cert_signature(CertSignature {
                signature,
                bitmap: vec![6],
            }),
            notar_aggregate: Some(invalid.final_aggregate),
        };
        for final_cert in [None, Some(cert)] {
            let footer = VersionedBlockFooter::V1(BlockFooterV1 {
                bank_hash: Hash::new_from_array([7; 32]),
                block_producer_time_nanos: 99,
                block_user_agent: b"agent".to_vec(),
                block_final_cert: final_cert,
                skip_reward_cert: Some(
                    SkipRewardCertificate::try_new(8, compressed, vec![1, 2]).unwrap(),
                ),
                notar_reward_cert: Some(
                    NotarRewardCertificate::try_new(
                        9,
                        Hash::new_from_array([3; 32]),
                        compressed,
                        vec![4, 5],
                    )
                    .unwrap(),
                ),
            });
            let replica = ReplicaBlockFooterInfo {
                slot: 10,
                block_footer: &footer,
            };
            let message = ProtobufMessage::BlockFooter {
                footer: &replica,
                bank_id: BANK_ID,
            };
            let created_at = SystemTime::now();
            let raw = message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(
                raw,
                message.encode_with_timestamp(ProtobufEncoder::Prost, created_at)
            );
            let Some(UpdateOneof::BlockFooter(footer)) = SubscribeUpdate::decode(raw.as_slice())
                .unwrap()
                .update_oneof
            else {
                panic!("expected footer")
            };
            assert!(footer.block_final_cert.is_none());
            assert_eq!((footer.slot, footer.bank_id), (10, BANK_ID));
            assert_eq!(footer.bank_hash, vec![7; 32]);
            let skip = footer.skip_reward_cert.unwrap();
            assert_eq!(skip.slot, 8);
            let skip = skip.aggregate.unwrap();
            assert_eq!(skip.signature_kind, 0);
            assert_eq!(skip.signature, compressed.0);
            assert!(skip.block_id.is_empty());
            assert_eq!(skip.signer_bitmap, vec![1, 2]);
            let notar = footer.notar_reward_cert.unwrap();
            assert_eq!(notar.slot, 9);
            let notar = notar.aggregate.unwrap();
            assert_eq!(notar.signature_kind, 0);
            assert_eq!(notar.signature, compressed.0);
            assert_eq!(notar.block_id, vec![3; 32]);
            assert_eq!(notar.signer_bitmap, vec![4, 5]);
        }
    }

    #[test]
    fn optional_bank_id_distinguishes_missing_from_zero() {
        let account = generate_accounts().into_iter().next().unwrap();
        let (slot, replica) = account.to_replica();
        let created_at = SystemTime::now();
        for bank_id in [None, Some(0), Some(BANK_ID), Some(u64::MAX)] {
            let account = ProtobufMessage::Account {
                slot,
                account: &replica,
                bank_id,
            };
            let slot = ProtobufMessage::Slot {
                slot,
                parent: Some(0),
                status:
                    &agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Processed,
                bank_id,
            };
            for message in [account, slot] {
                let raw = message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                assert_eq!(
                    raw,
                    message.encode_with_timestamp(ProtobufEncoder::Prost, created_at)
                );
                let update = SubscribeUpdate::decode(raw.as_slice())
                    .unwrap()
                    .update_oneof
                    .unwrap();
                match update {
                    UpdateOneof::Account(account) => assert_eq!(account.bank_id, bank_id),
                    UpdateOneof::Slot(slot) => assert_eq!(slot.bank_id, bank_id),
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn required_bank_ids_preserve_zero_and_maximum_values() {
        let block = generate_block_metas().into_iter().next().unwrap();
        let block = block.to_replica();
        let entry = generate_entries().into_iter().next().unwrap();
        let entry = entry.to_replica();
        let transaction = generate_transactions().into_iter().next().unwrap();
        let (slot, transaction) = transaction.to_replica();
        let created_at = SystemTime::now();
        for bank_id in [0, BANK_ID, u64::MAX] {
            for message in [
                ProtobufMessage::BlockMeta {
                    blockinfo: &block,
                    bank_id,
                },
                ProtobufMessage::Entry {
                    entry: &entry,
                    bank_id,
                },
                ProtobufMessage::Transaction {
                    slot,
                    transaction: &transaction,
                    bank_id,
                },
            ] {
                let raw = message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                assert_eq!(
                    raw,
                    message.encode_with_timestamp(ProtobufEncoder::Prost, created_at)
                );
                match SubscribeUpdate::decode(raw.as_slice())
                    .unwrap()
                    .update_oneof
                    .unwrap()
                {
                    UpdateOneof::BlockMeta(update) => assert_eq!(update.bank_id, bank_id),
                    UpdateOneof::Entry(update) => assert_eq!(update.bank_id, bank_id),
                    UpdateOneof::Transaction(update) => assert_eq!(update.bank_id, bank_id),
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn v1_config_presence_and_bank_id_survive_both_encoders() {
        use {
            solana_hash::Hash,
            solana_message::{
                VersionedMessage,
                v1::{Message, TransactionConfig},
            },
        };

        for config in [
            TransactionConfig::default(),
            TransactionConfig {
                priority_fee: Some(0),
                compute_unit_limit: Some(0),
                loaded_accounts_data_size_limit: Some(0),
                heap_size: Some(0),
            },
            TransactionConfig {
                priority_fee: Some(16_385),
                compute_unit_limit: Some(200_000),
                loaded_accounts_data_size_limit: Some(128_000),
                heap_size: Some(65_536),
            },
        ] {
            let mut fixture = generate_transactions().into_iter().next().unwrap();
            let previous = &fixture.versioned_transaction.message;
            fixture.versioned_transaction.message = VersionedMessage::V1(Message {
                header: *previous.header(),
                config,
                lifetime_specifier: Hash::new_from_array([7; 32]),
                account_keys: previous.static_account_keys().to_vec(),
                instructions: previous.instructions().to_vec(),
            });
            let (slot, transaction) = fixture.to_replica();
            let message = ProtobufMessage::Transaction {
                slot,
                transaction: &transaction,
                bank_id: BANK_ID,
            };
            let created_at = SystemTime::now();
            let raw = message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(
                raw,
                message.encode_with_timestamp(ProtobufEncoder::Prost, created_at)
            );
            let Some(UpdateOneof::Transaction(update)) = SubscribeUpdate::decode(raw.as_slice())
                .unwrap()
                .update_oneof
            else {
                panic!("expected transaction")
            };
            assert_eq!(update.bank_id, BANK_ID);
            let message = update
                .transaction
                .unwrap()
                .transaction
                .unwrap()
                .message
                .unwrap();
            assert!(message.versioned);
            assert!(message.address_table_lookups.is_empty());
            assert_eq!(message.recent_blockhash, vec![7; 32]);
            let encoded = message.config.unwrap();
            assert_eq!(encoded.priority_fee, config.priority_fee);
            assert_eq!(encoded.compute_unit_limit, config.compute_unit_limit);
            assert_eq!(
                encoded.loaded_accounts_data_size_limit,
                config.loaded_accounts_data_size_limit
            );
            assert_eq!(encoded.heap_size, config.heap_size);
        }
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
