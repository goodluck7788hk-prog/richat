use {
    super::*,
    futures::task::noop_waker,
    prost::Message as _,
    richat_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
    richat_shared::transports::{RecvStream, Subscribe},
    std::task::{Context, Poll},
};

fn plugin(config: Config) -> Plugin {
    Plugin {
        inner: Some(
            PluginInner::new(Config {
                channel: crate::config::ConfigChannel {
                    max_messages: 64,
                    max_bytes: 1_048_576,
                    ..config.channel
                },
                ..config
            })
            .unwrap(),
        ),
    }
}

fn next_update(stream: &mut RecvStream) -> UpdateOneof {
    let waker = noop_waker();
    let Poll::Ready(Some(Ok(bytes))) = stream.as_mut().poll_next(&mut Context::from_waker(&waker))
    else {
        panic!("expected callback notification");
    };
    SubscribeUpdate::decode(bytes.as_slice())
        .unwrap()
        .update_oneof
        .unwrap()
}

#[test]
fn bank_status_callback_preserves_bank_id() {
    let mut plugin = plugin(Config::default());
    let mut stream = plugin
        .inner
        .as_ref()
        .unwrap()
        .messages
        .subscribe(None, None)
        .unwrap();
    plugin
        .update_bank_status(10, Some(9), &SlotStatus::Processed, 16_385)
        .unwrap();
    let UpdateOneof::Slot(update) = next_update(&mut stream) else {
        panic!("expected slot");
    };
    assert_eq!(
        (update.slot, update.parent, update.bank_id),
        (10, Some(9), Some(16_385))
    );
    plugin.on_unload();
}

#[test]
fn optional_notifications_require_explicit_configuration() {
    let config: Config =
        serde_json::from_str(r#"{"block_footer":true,"entry_update_parent":true}"#).unwrap();
    let mut plugin = plugin(config);
    assert!(plugin.block_footer_notifications_enabled());
    plugin.on_unload();
}

#[test]
fn every_bank_callback_preserves_bank_id_for_both_encoders() {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions,
            ReplicaTransactionInfoVersions,
        },
        richat_benches::fixtures::{
            generate_accounts, generate_block_metas, generate_entries, generate_transactions,
        },
    };

    for encoder in [ProtobufEncoder::Raw, ProtobufEncoder::Prost] {
        let mut config = Config::default();
        config.channel.encoder = encoder;
        let mut plugin = plugin(config);
        let mut stream = plugin
            .inner
            .as_ref()
            .unwrap()
            .messages
            .subscribe(None, None)
            .unwrap();
        let account = generate_accounts().into_iter().next().unwrap();
        let (slot, account) = account.to_replica();
        plugin
            .update_account_for_bank(ReplicaAccountInfoVersions::V0_0_3(&account), slot, 16_385)
            .unwrap();
        let UpdateOneof::Account(update) = next_update(&mut stream) else {
            panic!("expected account")
        };
        assert_eq!((update.slot, update.bank_id), (slot, Some(16_385)));

        let transaction = generate_transactions().into_iter().next().unwrap();
        let (slot, transaction) = transaction.to_replica();
        plugin
            .notify_transaction_for_bank(
                ReplicaTransactionInfoVersions::V0_0_3(&transaction),
                slot,
                16_385,
            )
            .unwrap();
        let UpdateOneof::Transaction(update) = next_update(&mut stream) else {
            panic!("expected transaction")
        };
        assert_eq!((update.slot, update.bank_id), (slot, 16_385));

        let entry = generate_entries().into_iter().next().unwrap();
        let entry = entry.to_replica();
        plugin
            .notify_entry_for_bank(ReplicaEntryInfoVersions::V0_0_2(&entry), 16_385)
            .unwrap();
        let UpdateOneof::Entry(update) = next_update(&mut stream) else {
            panic!("expected entry")
        };
        assert_eq!((update.slot, update.bank_id), (entry.slot, 16_385));

        let block = generate_block_metas().into_iter().next().unwrap();
        let block = block.to_replica();
        plugin
            .notify_block_metadata_for_bank(ReplicaBlockInfoVersions::V0_0_4(&block), 16_385)
            .unwrap();
        let UpdateOneof::BlockMeta(update) = next_update(&mut stream) else {
            panic!("expected block meta")
        };
        assert_eq!((update.slot, update.bank_id), (block.slot, 16_385));
        plugin.on_unload();
    }
}

#[test]
fn legacy_slot_callback_keeps_bank_id_absent() {
    let mut plugin = plugin(Config::default());
    let mut stream = plugin
        .inner
        .as_ref()
        .unwrap()
        .messages
        .subscribe(None, None)
        .unwrap();
    plugin
        .update_slot_status(10, Some(9), &SlotStatus::FirstShredReceived)
        .unwrap();
    let UpdateOneof::Slot(update) = next_update(&mut stream) else {
        panic!("expected slot")
    };
    assert_eq!((update.slot, update.bank_id), (10, None));
    plugin.on_unload();
}

#[test]
fn optional_callbacks_emit_only_when_their_own_flag_is_enabled() {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaBlockFooterInfo, ReplicaEntryUpdateParentInfo,
        },
        solana_entry::block_component::{BlockFooterV1, VersionedBlockFooter},
        solana_hash::Hash,
    };

    let parent_block_id = Hash::new_from_array([9; 32]);
    let footer = VersionedBlockFooter::V1(BlockFooterV1 {
        bank_hash: Hash::new_from_array([7; 32]),
        block_producer_time_nanos: 99,
        block_user_agent: b"agent".to_vec(),
        block_final_cert: None,
        skip_reward_cert: None,
        notar_reward_cert: None,
    });
    let footer = ReplicaBlockFooterInfo {
        slot: 10,
        block_footer: &footer,
    };
    let parent = ReplicaEntryUpdateParentInfo {
        slot: 10,
        cleared_bank_id: 16_385,
        parent_slot: 9,
        parent_block_id: &parent_block_id,
    };

    for block_footer in [false, true] {
        for entry_update_parent in [false, true] {
            let mut plugin = plugin(Config {
                block_footer,
                entry_update_parent,
                ..Config::default()
            });
            let mut stream = plugin
                .inner
                .as_ref()
                .unwrap()
                .messages
                .subscribe(None, None)
                .unwrap();
            assert_eq!(plugin.block_footer_notifications_enabled(), block_footer);
            plugin
                .notify_block_footer(ReplicaBlockFooterInfoVersions::V0_0_1(&footer), 16_385)
                .unwrap();
            plugin
                .notify_entry_update_parent(ReplicaEntryUpdateParentInfoVersions::V0_0_1(&parent))
                .unwrap();
            if block_footer {
                let UpdateOneof::BlockFooter(update) = next_update(&mut stream) else {
                    panic!("expected footer")
                };
                assert_eq!((update.slot, update.bank_id), (10, 16_385));
                assert!(update.block_final_cert.is_none());
            }
            if entry_update_parent {
                let UpdateOneof::EntryUpdateParent(update) = next_update(&mut stream) else {
                    panic!("expected parent")
                };
                assert_eq!(
                    (update.slot, update.cleared_bank_id, update.parent_slot),
                    (10, 16_385, 9)
                );
                assert_eq!(update.parent_block_id, vec![9; 32]);
            }
            let waker = noop_waker();
            assert!(
                stream
                    .as_mut()
                    .poll_next(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            plugin.on_unload();
        }
    }
}

#[test]
fn optional_notifications_default_to_disabled() {
    let config: Config = serde_json::from_str("{}").unwrap();
    assert!(!config.block_footer);
    assert!(!config.entry_update_parent);
    assert!(!Plugin::default().block_footer_notifications_enabled());
}
