use {
    super::*,
    prost::Message as _,
    richat_proto::geyser::{
        SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
        SubscribeUpdateBlockFooter, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateEntryUpdateParent, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
    },
};

fn channel() -> (Messages, Sender) {
    let (mut messages, _) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages: 128,
            max_bytes: 1_048_576,
            storage: None,
        },
        true,
        true,
        true,
        CancellationToken::new(),
    )
    .unwrap();
    let (sender, _) = messages.to_sender(2).unwrap();
    (messages, sender)
}

fn push(sender: &mut Sender, source: &'static str, update: UpdateOneof) {
    let update = SubscribeUpdate {
        update_oneof: Some(update),
        created_at: Some(Default::default()),
        ..Default::default()
    };
    sender
        .push(
            DedupMode::SingleSource,
            source,
            Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost).unwrap(),
        )
        .unwrap();
}

fn status(bank_id: u64, status: SlotStatus) -> UpdateOneof {
    UpdateOneof::Slot(SubscribeUpdateSlot {
        slot: 100,
        status: status as i32,
        bank_id: Some(bank_id),
        ..Default::default()
    })
}

fn account(bank_id: u64, lamports: u64) -> UpdateOneof {
    UpdateOneof::Account(SubscribeUpdateAccount {
        slot: 100,
        bank_id: Some(bank_id),
        account: Some(SubscribeUpdateAccountInfo {
            pubkey: vec![1; 32],
            owner: vec![2; 32],
            lamports,
            write_version: lamports,
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn entry(bank_id: u64) -> UpdateOneof {
    UpdateOneof::Entry(SubscribeUpdateEntry {
        slot: 100,
        bank_id,
        hash: vec![3; 32],
        ..Default::default()
    })
}

fn meta(bank_id: u64) -> UpdateOneof {
    UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
        slot: 100,
        bank_id,
        entries_count: 1,
        block_height: Some(
            richat_proto::solana::storage::confirmed_block::BlockHeight { block_height: 100 },
        ),
        blockhash: "11111111111111111111111111111111".to_owned(),
        ..Default::default()
    })
}

fn updates(messages: &Messages, commitment: CommitmentLevel) -> Vec<UpdateOneof> {
    let receiver = messages.to_receiver();
    let mut cursor = 129;
    let mut updates = vec![];
    while let Some(message) = receiver.try_recv(commitment, cursor).unwrap() {
        let encoded = FilteredUpdate {
            filters: Default::default(),
            filtered_update: MessageRef::from(&message).into(),
        }
        .encode_to_vec();
        updates.push(
            SubscribeUpdate::decode(encoded.as_slice())
                .unwrap()
                .update_oneof
                .unwrap(),
        );
        cursor += 1;
    }
    updates
}

#[test]
fn independent_banks_do_not_mix_and_cleared_bank_cannot_resume() {
    let (messages, mut sender) = channel();
    for update in [
        status(11, SlotStatus::SlotCreatedBank),
        account(11, 11),
        entry(11),
        meta(11),
    ] {
        push(&mut sender, "one", update);
    }
    let parent = UpdateOneof::EntryUpdateParent(SubscribeUpdateEntryUpdateParent {
        slot: 100,
        cleared_bank_id: 11,
        parent_slot: 99,
        parent_block_id: vec![4; 32],
    });
    push(&mut sender, "one", parent.clone());
    push(&mut sender, "one", parent);
    push(&mut sender, "one", account(11, 999));
    for update in [
        status(22, SlotStatus::SlotCreatedBank),
        account(22, 22),
        entry(22),
        meta(22),
        UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 100,
            bank_id: 22,
            bank_hash: vec![5; 32],
            ..Default::default()
        }),
        status(22, SlotStatus::SlotConfirmed),
        status(22, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "one", update);
    }
    let processed = updates(&messages, CommitmentLevel::Processed);
    assert_eq!(
        processed
            .iter()
            .filter(|update| matches!(update, UpdateOneof::EntryUpdateParent(_)))
            .count(),
        1
    );
    assert!(!processed.iter().any(|update| matches!(update, UpdateOneof::Account(account) if account.account.as_ref().unwrap().lamports == 999)));
    for commitment in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
        let updates = updates(&messages, commitment);
        let blocks: Vec<_> = updates
            .iter()
            .filter_map(|update| {
                if let UpdateOneof::Block(block) = update {
                    Some(block)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].bank_id, 22);
        assert_eq!(blocks[0].accounts.len(), 1);
        assert_eq!(blocks[0].accounts[0].lamports, 22);
        assert_eq!(blocks[0].entries[0].bank_id, 22);
        assert_eq!(
            updates
                .iter()
                .filter(|update| matches!(update, UpdateOneof::BlockFooter(_)))
                .count(),
            1
        );
        assert!(
            !updates
                .iter()
                .any(|update| matches!(update, UpdateOneof::EntryUpdateParent(_)))
        );
    }
}

#[test]
fn equal_numeric_bank_ids_from_different_nodes_are_not_combined() {
    let (messages, mut sender) = channel();
    for update in [
        status(7, SlotStatus::SlotCreatedBank),
        account(7, 7),
        entry(7),
    ] {
        push(&mut sender, "one", update);
    }
    for update in [
        account(7, 777),
        entry(7),
        meta(7),
        status(7, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "two", update);
    }
    for update in [
        meta(7),
        status(7, SlotStatus::SlotConfirmed),
        status(7, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "one", update);
    }
    let updates = updates(&messages, CommitmentLevel::Finalized);
    let blocks: Vec<_> = updates
        .iter()
        .filter_map(|update| {
            if let UpdateOneof::Block(block) = update {
                Some(block)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].accounts.len(), 1);
    assert_eq!(blocks[0].accounts[0].lamports, 7);
}

#[test]
fn late_footer_after_finalization_is_delivered_once_to_each_commitment() {
    let (messages, mut sender) = channel();
    for update in [
        status(7, SlotStatus::SlotCreatedBank),
        account(7, 7),
        entry(7),
        meta(7),
        status(7, SlotStatus::SlotConfirmed),
        status(7, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "one", update);
    }
    let footer = UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
        slot: 100,
        bank_id: 7,
        bank_hash: vec![5; 32],
        ..Default::default()
    });
    push(&mut sender, "one", footer.clone());
    push(&mut sender, "one", footer);
    for commitment in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        assert_eq!(
            updates(&messages, commitment)
                .iter()
                .filter(|update| matches!(update, UpdateOneof::BlockFooter(_)))
                .count(),
            1
        );
    }
}

#[test]
fn replay_identity_keeps_distinct_banks_and_unsigned_write_versions() {
    let identity = |update| {
        let update = SubscribeUpdate {
            update_oneof: Some(update),
            created_at: Some(Default::default()),
            ..Default::default()
        };
        let message: ParsedMessage =
            Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
                .unwrap()
                .into();
        message
    };
    let first = identity(account(11, 10));
    let newer = identity(account(11, 11));
    let other_bank = identity(account(22, 10));
    let hasher = RandomState::default();
    assert_ne!(
        first.get_id(hasher.build_hasher()),
        newer.get_id(hasher.build_hasher())
    );
    assert_ne!(
        first.get_id(hasher.build_hasher()),
        other_bank.get_id(hasher.build_hasher())
    );
}

#[test]
fn ambiguous_unbanked_commitment_waits_for_an_explicit_bank() {
    let (messages, mut sender) = channel();
    for bank_id in [10_000, 2] {
        for update in [
            status(bank_id, SlotStatus::SlotCreatedBank),
            account(bank_id, bank_id),
            entry(bank_id),
            meta(bank_id),
        ] {
            push(&mut sender, "one", update);
        }
    }
    for status in [SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized] {
        push(
            &mut sender,
            "one",
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                status: status as i32,
                bank_id: None,
                ..Default::default()
            }),
        );
    }
    assert!(
        updates(&messages, CommitmentLevel::Finalized)
            .iter()
            .all(|message| matches!(message, UpdateOneof::Slot(_)))
    );
    assert_eq!(sender.global_replay_from_slot.load(), None);
    assert!(!updates(&messages, CommitmentLevel::Processed).iter().any(|message|
        matches!(message, UpdateOneof::Slot(slot) if slot.status == SlotStatus::SlotFinalized as i32 && slot.bank_id.is_none())));
    for update in [
        status(2, SlotStatus::SlotConfirmed),
        status(2, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "one", update);
    }
    let final_updates = updates(&messages, CommitmentLevel::Finalized);
    assert!(
        final_updates
            .iter()
            .any(|message| matches!(message, UpdateOneof::Block(block) if block.bank_id == 2))
    );
    assert_eq!(sender.global_replay_from_slot.load(), Some(101));
}

#[test]
fn finalized_bank_history_is_compact_and_bounded_by_retained_slots() {
    let (_, mut sender) = channel();
    for slot in 1..=512 {
        for mut update in [
            status(slot, SlotStatus::SlotCreatedBank),
            entry(slot),
            meta(slot),
            status(slot, SlotStatus::SlotFinalized),
        ] {
            match &mut update {
                UpdateOneof::Slot(message) => message.slot = slot,
                UpdateOneof::Entry(message) => message.slot = slot,
                UpdateOneof::BlockMeta(message) => message.slot = slot,
                _ => unreachable!(),
            }
            push(&mut sender, "one", update);
        }
    }
    assert!(
        sender.slots.len() <= 34,
        "retained {} slots",
        sender.slots.len()
    );
    assert!(sender.slots.values().all(|slot| slot.finalized
        && slot.banks.values().all(|bank| bank.messages.is_empty()
            && bank.messages.capacity() == 0
            && bank.accounts_dedup.is_empty()
            && bank.accounts_dedup.capacity() == 0
            && bank.block_meta.is_none())));
}

#[test]
fn newer_owner_cannot_finalize_past_another_owners_unresolved_slot() {
    let (_, mut sender) = channel();
    for update in [status(7, SlotStatus::SlotCreatedBank), account(7, 7)] {
        push(&mut sender, "one", update);
    }
    let mut later = status(8, SlotStatus::SlotFinalized);
    let UpdateOneof::Slot(message) = &mut later else {
        unreachable!()
    };
    message.slot = 101;
    let update = SubscribeUpdate {
        update_oneof: Some(later),
        created_at: Some(Default::default()),
        ..Default::default()
    };
    let result = sender.push(
        DedupMode::SingleSource,
        "two",
        Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost).unwrap(),
    );
    assert!(result.is_err());
    assert_eq!(sender.global_replay_from_slot.load(), None);
    assert!(sender.slots.contains_key(&100));
}

#[test]
fn expired_history_cannot_be_recreated_from_only_a_late_footer() {
    let (messages, mut sender) = channel();
    for update in [
        status(7, SlotStatus::SlotCreatedBank),
        account(7, 7),
        entry(7),
        meta(7),
        status(7, SlotStatus::SlotConfirmed),
        status(7, SlotStatus::SlotFinalized),
    ] {
        push(&mut sender, "one", update);
    }
    sender.processed.bytes_max = 64;
    for slot in 101..=104 {
        let mut update = account(slot, slot);
        let UpdateOneof::Account(message) = &mut update else {
            unreachable!()
        };
        message.slot = slot;
        push(&mut sender, "one", update);
    }
    assert!(!messages.shared_processed.slots_lock().contains_key(&100));
    sender.processed.bytes_max = 1_048_576;
    push(
        &mut sender,
        "one",
        UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 100,
            bank_id: 7,
            ..Default::default()
        }),
    );
    assert!(messages.subscribe(Some(100), None).is_err());
}

#[test]
fn status_only_unbanked_notifications_do_not_create_a_phantom_candidate() {
    let (messages, mut sender) = channel();
    push(
        &mut sender,
        "one",
        UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 100,
            status: SlotStatus::SlotFirstShredReceived as i32,
            ..Default::default()
        }),
    );
    for update in [
        status(7, SlotStatus::SlotCreatedBank),
        account(7, 7),
        entry(7),
        meta(7),
    ] {
        push(&mut sender, "one", update);
    }
    for status in [SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized] {
        push(
            &mut sender,
            "one",
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                status: status as i32,
                ..Default::default()
            }),
        );
    }
    assert!(
        updates(&messages, CommitmentLevel::Finalized)
            .iter()
            .any(|message| matches!(message, UpdateOneof::Block(block) if block.bank_id == 7))
    );
}

#[test]
fn diagnostic_failure_flag_cannot_bypass_cross_owner_finalization_fence() {
    let (_, mut sender) = channel();
    for update in [
        status(7, SlotStatus::SlotCreatedBank),
        account(7, 7),
        entry(7),
        meta(7),
        status(7, SlotStatus::SlotConfirmed),
    ] {
        push(&mut sender, "one", update);
    }
    let update = SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 101,
            status: SlotStatus::SlotFinalized as i32,
            bank_id: Some(8),
            ..Default::default()
        })),
        created_at: Some(Default::default()),
        ..Default::default()
    };
    assert!(
        sender
            .push(
                DedupMode::SingleSource,
                "two",
                Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
                    .unwrap()
            )
            .is_err()
    );
    assert_eq!(sender.global_replay_from_slot.load(), None);
}

#[test]
fn unbanked_new_owner_finalization_cannot_bypass_pending_owner_fence() {
    let (_, mut sender) = channel();
    push(&mut sender, "one", status(7, SlotStatus::SlotCreatedBank));
    let update = SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 101,
            status: SlotStatus::SlotFinalized as i32,
            ..Default::default()
        })),
        created_at: Some(Default::default()),
        ..Default::default()
    };
    assert!(
        sender
            .push(
                DedupMode::SingleSource,
                "two",
                Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
                    .unwrap()
            )
            .is_err()
    );
    assert_eq!(sender.global_replay_from_slot.load(), None);
}

#[test]
fn restored_owner_cannot_cross_its_own_unresolved_recovery_barrier() {
    let (_, mut sender) = channel();
    sender.recovered_sources.insert(100, Arc::from("one"));
    let update = SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 101,
            status: SlotStatus::SlotFinalized as i32,
            bank_id: Some(33),
            ..Default::default()
        })),
        created_at: Some(Default::default()),
        ..Default::default()
    };
    assert!(
        sender
            .push(
                DedupMode::SingleSource,
                "one",
                Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
                    .unwrap()
            )
            .is_err()
    );
    assert_eq!(sender.global_replay_from_slot.load(), None);
}
