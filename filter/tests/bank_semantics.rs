use {
    maplit::{hashmap, hashset},
    prost::Message as _,
    richat_filter::{
        config::{ConfigFilter, ConfigFilterAccountsDataSlice, ConfigLimits},
        filter::Filter,
        message::{Message, MessageParserEncoding},
    },
    richat_proto::{
        geyser::{
            BlockFooterFinalCert, BlockFooterNotarRewardCert, BlockFooterSkipRewardCert,
            BlockFooterVotesAggregate, SlotStatus, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlockFooter,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateAccount,
            SubscribeUpdateAccountInfo, SubscribeUpdateBlock, SubscribeUpdateBlockFooter,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateEntryUpdateParent,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            subscribe_update::UpdateOneof,
        },
        solana::storage::confirmed_block::{
            BlockHeight, Message as TransactionMessage, Rewards, Transaction, TransactionConfig,
            TransactionStatusMeta,
        },
    },
    solana_commitment_config::CommitmentLevel,
    std::sync::Arc,
};

const BANK_ID: u64 = 123_456_789;
const PARSERS: [MessageParserEncoding; 2] =
    [MessageParserEncoding::Limited, MessageParserEncoding::Prost];

fn envelope(update_oneof: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![],
        update_oneof: Some(update_oneof),
        created_at: Some(prost_types::Timestamp {
            seconds: 17,
            nanos: 29,
        }),
    }
}

fn parse(update: &SubscribeUpdate, parser: MessageParserEncoding) -> Message {
    Message::parse(update.encode_to_vec().into(), parser).expect("valid update")
}

fn filtered(filter: &Filter, message: &Message) -> Vec<SubscribeUpdate> {
    filter
        .get_updates(message, CommitmentLevel::Processed)
        .into_iter()
        .map(|update| SubscribeUpdate::decode(update.encode_to_vec().as_slice()).unwrap())
        .collect()
}

fn account(bank_id: Option<u64>) -> UpdateOneof {
    UpdateOneof::Account(SubscribeUpdateAccount {
        account: Some(SubscribeUpdateAccountInfo {
            pubkey: vec![1; 32],
            owner: vec![2; 32],
            lamports: 42,
            executable: true,
            rent_epoch: 8,
            data: vec![1, 2, 3, 4, 5],
            write_version: 71,
            txn_signature: Some(vec![3; 64]),
        }),
        slot: 10,
        is_startup: true,
        bank_id,
    })
}

fn transaction(bank_id: u64) -> UpdateOneof {
    UpdateOneof::Transaction(SubscribeUpdateTransaction {
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: vec![3; 64],
            is_vote: true,
            transaction: Some(Transaction {
                signatures: vec![vec![3; 64]],
                message: Some(TransactionMessage {
                    account_keys: vec![vec![4; 32]],
                    ..Default::default()
                }),
            }),
            meta: Some(TransactionStatusMeta::default()),
            index: 9,
        }),
        slot: 10,
        bank_id,
    })
}

#[test]
fn sliced_accounts_preserve_every_field_and_bank_presence() {
    let filter = Filter::new(&ConfigFilter {
        accounts: hashmap! { "account".into() => Default::default() },
        accounts_data_slice: vec![ConfigFilterAccountsDataSlice {
            offset: 1,
            length: 3,
        }],
        ..Default::default()
    });
    for parser in PARSERS {
        for bank_id in [None, Some(0), Some(BANK_ID), Some(u64::MAX)] {
            let update = envelope(account(bank_id));
            let mut expected = update.clone();
            expected.filters = vec!["account".into()];
            if let Some(UpdateOneof::Account(account)) = &mut expected.update_oneof {
                account.account.as_mut().unwrap().data = vec![2, 3, 4];
            }
            assert_eq!(
                filtered(&filter, &parse(&update, parser)),
                vec![expected],
                "{parser:?}"
            );
        }
    }
}

#[test]
fn slot_and_transaction_filters_preserve_bank_ids() {
    let request = SubscribeRequest {
        slots: hashmap! { "slot".into() => SubscribeRequestFilterSlots::default() },
        accounts: hashmap! { "account".into() => SubscribeRequestFilterAccounts::default() },
        transactions: hashmap! { "tx".into() => SubscribeRequestFilterTransactions::default() },
        transactions_status: hashmap! { "status".into() => SubscribeRequestFilterTransactions::default() },
        ..Default::default()
    };
    let filter = Filter::new(&request.try_into().unwrap());
    for parser in PARSERS {
        for bank_id in [0, BANK_ID, u64::MAX] {
            for (name, update_oneof) in [
                (
                    "slot",
                    UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot: 10,
                        parent: Some(9),
                        status: SlotStatus::SlotConfirmed as i32,
                        bank_id: Some(bank_id),
                        ..Default::default()
                    }),
                ),
                ("account", account(Some(bank_id))),
                ("tx", transaction(bank_id)),
            ] {
                let update = envelope(update_oneof);
                let output = filtered(&filter, &parse(&update, parser));
                let mut expected = update.clone();
                expected.filters = vec![name.into()];
                assert_eq!(output[0], expected, "{parser:?}");
                if name == "tx" {
                    assert_eq!(output.len(), 2);
                    let Some(UpdateOneof::TransactionStatus(status)) = &output[1].update_oneof
                    else {
                        panic!("expected transaction status");
                    };
                    assert_eq!(status.bank_id, bank_id, "{parser:?}");
                    assert_eq!((status.slot, status.index, status.is_vote), (10, 9, true));
                    assert_eq!(status.signature, vec![3; 64]);
                }
            }
        }
    }
}

#[test]
fn footer_and_parent_updates_reach_only_requested_filters() {
    let footer = envelope(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
        slot: 10,
        bank_id: BANK_ID,
        bank_hash: vec![1; 32],
        ..Default::default()
    }));
    let parent = envelope(UpdateOneof::EntryUpdateParent(
        SubscribeUpdateEntryUpdateParent {
            slot: 10,
            cleared_bank_id: BANK_ID,
            parent_slot: 9,
            parent_block_id: vec![2; 32],
        },
    ));
    let request = SubscribeRequest {
        entry: hashmap! {
            "regular".into() => SubscribeRequestFilterEntry::default(),
            "parents".into() => SubscribeRequestFilterEntry { include_update_parent: Some(true) },
        },
        block_footer: hashmap! { "footer".into() => SubscribeRequestFilterBlockFooter::default() },
        ..Default::default()
    };
    let filter = Filter::new(&request.try_into().unwrap());
    for parser in PARSERS {
        for (name, input) in [("footer", &footer), ("parents", &parent)] {
            let message = parse(input, parser);
            let mut expected = input.clone();
            expected.filters = vec![name.into()];
            assert_eq!(filtered(&filter, &message), vec![expected], "{parser:?}");
            assert!(filtered(&Filter::default(), &message).is_empty());
        }
        let entry = envelope(UpdateOneof::Entry(SubscribeUpdateEntry {
            slot: 10,
            bank_id: BANK_ID,
            ..Default::default()
        }));
        let updates = filtered(&filter, &parse(&entry, parser));
        assert_eq!(updates.len(), 1);
        let mut names = updates[0].filters.clone();
        names.sort();
        assert_eq!(names, ["parents", "regular"]);
        assert_eq!(updates[0].update_oneof, entry.update_oneof);
        assert_eq!(updates[0].created_at, entry.created_at);
    }
}

#[test]
fn subscription_roundtrip_retains_new_opt_ins() {
    let request = SubscribeRequest {
        entry: hashmap! {
            "regular".into() => SubscribeRequestFilterEntry::default(),
            "parents".into() => SubscribeRequestFilterEntry { include_update_parent: Some(true) },
        },
        block_footer: hashmap! {
            "certificates".into() => SubscribeRequestFilterBlockFooter { include_certificates: Some(true) },
            "no-certificates".into() => SubscribeRequestFilterBlockFooter { include_certificates: Some(false) },
        },
        ..Default::default()
    };
    let config = ConfigFilter::try_from(request.clone()).unwrap();
    assert_eq!(SubscribeRequest::from(config), request);
}

#[test]
fn footer_filter_names_obey_subscription_limits() {
    let request = SubscribeRequest {
        block_footer: hashmap! { "too-long".into() => SubscribeRequestFilterBlockFooter::default() },
        ..Default::default()
    };
    let config = ConfigFilter::try_from(request).unwrap();
    assert!(
        ConfigLimits {
            name_max: 4,
            ..Default::default()
        }
        .check_filter(&config)
        .is_err()
    );
}

#[test]
fn footer_filter_count_obeys_configured_limit() {
    let request = SubscribeRequest {
        block_footer: hashmap! {
            "first".into() => SubscribeRequestFilterBlockFooter::default(),
            "second".into() => SubscribeRequestFilterBlockFooter::default(),
        },
        ..Default::default()
    };
    let config = ConfigFilter::try_from(request).unwrap();
    let mut limits = ConfigLimits::default();
    limits.block_footer.max = 1;
    assert!(limits.check_filter(&config).is_err());
    limits.block_footer.max = 2;
    assert!(limits.check_filter(&config).is_ok());
}

#[test]
fn legacy_updates_accept_unknown_top_level_wire_types() {
    for parser in PARSERS {
        let original = envelope(account(None));
        for unknown in [
            vec![0xa0, 0x06, 0x01],
            vec![0xa9, 0x06, 1, 2, 3, 4, 5, 6, 7, 8],
            vec![0xb2, 0x06, 2, 4, 5],
        ] {
            let mut bytes = original.encode_to_vec();
            bytes.extend(unknown);
            let message = Message::parse(bytes.into(), parser).expect("unknown fields are skipped");
            let filter = Filter::new(&ConfigFilter {
                accounts: hashmap! { "account".into() => Default::default() },
                ..Default::default()
            });
            let mut expected = original.clone();
            expected.filters = vec!["account".into()];
            assert_eq!(filtered(&filter, &message), vec![expected]);
        }
    }
}

#[test]
fn footer_certificates_are_stripped_only_for_filters_without_opt_in() {
    let aggregate = BlockFooterVotesAggregate {
        signature_kind: 0,
        signature: vec![2; 96],
        block_id: vec![3; 32],
        signer_bitmap: vec![1, 8, 0, 255],
    };
    let footer = SubscribeUpdateBlockFooter {
        slot: 10,
        bank_id: BANK_ID,
        bank_hash: vec![1; 32],
        block_producer_time_nanos: 17,
        block_user_agent: b"test-agent".to_vec(),
        block_final_cert: Some(BlockFooterFinalCert {
            slot: 10,
            final_aggregate: Some(aggregate.clone()),
            notar_aggregate: Some(aggregate.clone()),
        }),
        skip_reward_cert: Some(BlockFooterSkipRewardCert {
            slot: 9,
            aggregate: Some(aggregate.clone()),
        }),
        notar_reward_cert: Some(BlockFooterNotarRewardCert {
            slot: 10,
            aggregate: Some(aggregate),
        }),
    };
    let request = SubscribeRequest {
        block_footer: hashmap! {
            "with".into() => SubscribeRequestFilterBlockFooter { include_certificates: Some(true) },
            "without".into() => SubscribeRequestFilterBlockFooter { include_certificates: Some(false) },
            "default".into() => SubscribeRequestFilterBlockFooter::default(),
        },
        ..Default::default()
    };
    let filter = Filter::new(&request.try_into().unwrap());
    for parser in PARSERS {
        for input_footer in [
            footer.clone(),
            SubscribeUpdateBlockFooter {
                block_final_cert: None,
                skip_reward_cert: None,
                notar_reward_cert: None,
                ..footer.clone()
            },
        ] {
            let input = envelope(UpdateOneof::BlockFooter(input_footer.clone()));
            let message = parse(&input, parser);
            assert_eq!(message.bank_id(), Some(BANK_ID));
            assert_eq!(message.slot(), 10);
            assert_eq!(
                prost_types::Timestamp::from(message.created_at()),
                input.created_at.unwrap()
            );
            let output = filtered(&filter, &message);
            assert_eq!(output.len(), 2);
            for update in output {
                let mut expected = input_footer.clone();
                if update.filters == ["with"] {
                    assert_eq!(update.filters.len(), 1);
                } else {
                    let mut names = update.filters.clone();
                    names.sort();
                    assert_eq!(names, ["default", "without"]);
                    expected.block_final_cert = None;
                    expected.skip_reward_cert = None;
                    expected.notar_reward_cert = None;
                }
                assert_eq!(update.created_at, input.created_at);
                assert_eq!(
                    update.update_oneof,
                    Some(UpdateOneof::BlockFooter(expected)),
                    "{parser:?}"
                );
            }
        }
    }
}

#[test]
fn assembled_blocks_preserve_bank_ids_and_component_fields() {
    let meta = SubscribeUpdateBlockMeta {
        slot: 10,
        blockhash: "blockhash".into(),
        parent_slot: 9,
        parent_blockhash: "parent".into(),
        rewards: Some(Rewards::default()),
        block_height: Some(BlockHeight { block_height: 8 }),
        executed_transaction_count: 1,
        entries_count: 1,
        bank_id: BANK_ID,
        ..Default::default()
    };
    let entry = SubscribeUpdateEntry {
        slot: 10,
        index: 3,
        num_hashes: 4,
        hash: vec![4; 32],
        executed_transaction_count: 1,
        starting_transaction_index: 9,
        bank_id: BANK_ID,
    };
    let request = SubscribeRequest {
        blocks: hashmap! { "block".into() => SubscribeRequestFilterBlocks { include_accounts: Some(true), include_entries: Some(true), include_transactions: Some(true), ..Default::default() } },
        ..Default::default()
    };
    let mut config = ConfigFilter::try_from(request).unwrap();
    config.accounts_data_slice = vec![ConfigFilterAccountsDataSlice {
        offset: 1,
        length: 3,
    }];
    let filter = Filter::new(&config);
    let UpdateOneof::Account(expected_account) = account(Some(BANK_ID)) else {
        panic!("account");
    };
    let mut expected_account = expected_account.account.unwrap();
    expected_account.data = vec![2, 3, 4];
    let UpdateOneof::Transaction(expected_transaction) = transaction(BANK_ID) else {
        panic!("transaction");
    };
    let expected = SubscribeUpdateBlock {
        slot: 10,
        blockhash: "blockhash".into(),
        rewards: Some(Rewards::default()),
        block_time: None,
        block_height: Some(BlockHeight { block_height: 8 }),
        parent_slot: 9,
        parent_blockhash: "parent".into(),
        executed_transaction_count: 1,
        transactions: vec![expected_transaction.transaction.unwrap()],
        updated_account_count: 1,
        accounts: vec![expected_account],
        entries_count: 1,
        entries: vec![entry.clone()],
        bank_id: BANK_ID,
    };
    for parser in PARSERS {
        let Message::Account(account) = parse(&envelope(account(Some(BANK_ID))), parser) else {
            panic!("account");
        };
        let Message::Transaction(transaction) = parse(&envelope(transaction(BANK_ID)), parser)
        else {
            panic!("transaction");
        };
        let Message::Entry(parsed_entry) =
            parse(&envelope(UpdateOneof::Entry(entry.clone())), parser)
        else {
            panic!("entry");
        };
        let Message::BlockMeta(parsed_meta) =
            parse(&envelope(UpdateOneof::BlockMeta(meta.clone())), parser)
        else {
            panic!("block meta");
        };
        assert_eq!(account.bank_id(), Some(BANK_ID));
        assert_eq!(transaction.bank_id(), BANK_ID);
        assert_eq!(parsed_entry.bank_id(), BANK_ID);
        assert_eq!(parsed_meta.bank_id(), BANK_ID);
        let created_at = account.created_at();
        let block = Message::create_block(
            vec![Arc::new(account)],
            vec![Arc::new(transaction)],
            vec![Arc::new(parsed_entry)],
            Arc::new(parsed_meta),
            created_at,
        )
        .unwrap();
        assert_eq!(block.bank_id(), Some(BANK_ID));
        let updates = filtered(&filter, &block);
        assert_eq!(updates.len(), 1);
        let Some(UpdateOneof::Block(output)) = &updates[0].update_oneof else {
            panic!("block");
        };
        assert_eq!(output, &expected, "{parser:?}");
        assert_eq!(updates[0].filters, ["block"]);
        assert_eq!(updates[0].created_at, Some(created_at.into()));
        let reparsed = parse(
            &envelope(UpdateOneof::Block(output.clone())),
            MessageParserEncoding::Prost,
        );
        assert_eq!(reparsed.bank_id(), Some(BANK_ID));
        let mut full_config = config.clone();
        full_config.accounts_data_slice.clear();
        let full_filter = Filter::new(&full_config);
        assert_eq!(
            filtered(&full_filter, &reparsed)[0].update_oneof,
            Some(UpdateOneof::Block(expected.clone()))
        );
    }
}

#[test]
fn bank_accessors_preserve_absence_and_explicit_zero() {
    for parser in PARSERS {
        for bank_id in [None, Some(0), Some(BANK_ID), Some(u64::MAX)] {
            for update in [
                account(bank_id),
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    bank_id,
                    ..Default::default()
                }),
            ] {
                assert_eq!(
                    parse(&envelope(update), parser).bank_id(),
                    bank_id,
                    "{parser:?}"
                );
            }
        }
        let message = parse(
            &envelope(UpdateOneof::EntryUpdateParent(
                SubscribeUpdateEntryUpdateParent {
                    slot: 10,
                    cleared_bank_id: BANK_ID,
                    parent_slot: 9,
                    parent_block_id: vec![1; 32],
                },
            )),
            parser,
        );
        assert_eq!(message.bank_id(), Some(BANK_ID));
    }
}

#[test]
fn legacy_footer_certificate_bytes_are_skipped_safely() {
    let footer = SubscribeUpdateBlockFooter {
        slot: 10,
        bank_id: BANK_ID,
        bank_hash: vec![1; 32],
        ..Default::default()
    };
    let mut payload = footer.encode_to_vec();
    payload.extend([0x32, 2, 0xff, 0xff, 0x3a, 0, 0x42, 1, 0xfe]);
    let mut bytes = vec![0x62];
    prost::encoding::encode_varint(payload.len() as u64, &mut bytes);
    bytes.extend(payload);
    bytes.extend([0x5a, 0]);
    for parser in PARSERS {
        let message = Message::parse(bytes.clone().into(), parser).unwrap();
        let Message::BlockFooter(message) = message else {
            panic!("footer");
        };
        assert_eq!(message.footer(), &footer);
    }
}

#[test]
fn malformed_new_and_unknown_fields_return_errors() {
    for parser in PARSERS {
        for bytes in [
            vec![0x62, 0xff],
            vec![0x68, 0],
            vec![0x6a, 3, 0x10, 0x80, 0x80],
            vec![0xa0, 0x06, 0x80],
            vec![0xa9, 0x06, 1],
        ] {
            assert!(Message::parse(bytes.into(), parser).is_err(), "{parser:?}");
        }
    }
}

#[test]
fn parent_filter_names_and_counts_obey_entry_limits() {
    let filter = ConfigFilter {
        entries_update_parent: hashset! { "too-long".into() },
        ..Default::default()
    };
    assert!(
        ConfigLimits {
            name_max: 4,
            ..Default::default()
        }
        .check_filter(&filter)
        .is_err()
    );
    let mut limits = ConfigLimits::default();
    limits.entries.max = 0;
    assert!(limits.check_filter(&filter).is_err());
}

#[test]
fn dead_slot_error_accessor_matches_prost_fields() {
    for parser in PARSERS {
        let update = envelope(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 10,
            parent: Some(9),
            status: SlotStatus::SlotDead as i32,
            dead_error: Some("bank rejected".into()),
            bank_id: Some(BANK_ID),
        }));
        let Message::Slot(slot) = parse(&update, parser) else {
            panic!("slot");
        };
        assert_eq!(slot.dead_error(), Some("bank rejected"), "{parser:?}");
    }
}

#[test]
fn invalid_dead_slot_utf8_is_rejected_before_access() {
    for parser in PARSERS {
        let bytes = vec![0x1a, 3, 0x22, 1, 0xff, 0x5a, 0];
        assert!(Message::parse(bytes.into(), parser).is_err(), "{parser:?}");
    }
}

#[test]
fn transaction_versions_and_v1_config_survive_filtering() {
    let filter = Filter::new(&ConfigFilter {
        transactions: hashmap! { "transaction".into() => Default::default() },
        ..Default::default()
    });
    for parser in PARSERS {
        for (versioned, config) in [
            (false, None),
            (true, None),
            (
                true,
                Some(TransactionConfig {
                    priority_fee: Some(123_456),
                    compute_unit_limit: Some(987_654),
                    loaded_accounts_data_size_limit: Some(12_345),
                    heap_size: Some(65_536),
                }),
            ),
        ] {
            let mut input = envelope(transaction(BANK_ID));
            let Some(UpdateOneof::Transaction(transaction)) = &mut input.update_oneof else {
                panic!("transaction");
            };
            let message = transaction
                .transaction
                .as_mut()
                .unwrap()
                .transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap();
            message.versioned = versioned;
            message.config = config;
            let parsed = parse(&input, parser);
            let mut expected = input.clone();
            expected.filters = vec!["transaction".into()];
            assert_eq!(filtered(&filter, &parsed), vec![expected], "{parser:?}");
        }
    }
}
