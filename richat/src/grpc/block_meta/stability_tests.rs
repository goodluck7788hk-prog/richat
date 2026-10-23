use {
    super::*,
    prost::Message as _,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_proto::{
        geyser::{
            SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateEntryUpdateParent,
            SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
        solana::storage::confirmed_block::BlockHeight,
    },
    std::borrow::Cow,
};

fn parse(update: UpdateOneof) -> ParsedMessage {
    Message::parse(
        Cow::Owned(
            SubscribeUpdate {
                update_oneof: Some(update),
                created_at: Some(Default::default()),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        MessageParserEncoding::Prost,
    )
    .unwrap()
    .into()
}

fn block_meta(slot: Slot, height: Slot, hash: &str) -> ParsedMessage {
    parse(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
        slot,
        blockhash: hash.to_owned(),
        block_height: Some(BlockHeight {
            block_height: height,
        }),
        ..Default::default()
    }))
}

fn status(slot: Slot, status: SlotStatus) -> ParsedMessage {
    parse(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        status: status as i32,
        ..Default::default()
    }))
}

fn bank_meta(slot: Slot, bank_id: u64, hash: &str) -> ParsedMessage {
    parse(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
        slot,
        bank_id,
        blockhash: hash.to_owned(),
        block_height: Some(BlockHeight { block_height: slot }),
        ..Default::default()
    }))
}

fn bank_status(slot: Slot, bank_id: u64, status: SlotStatus) -> ParsedMessage {
    parse(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        bank_id: Some(bank_id),
        status: status as i32,
        ..Default::default()
    }))
}

fn clear_bank(slot: Slot, bank_id: u64) -> ParsedMessage {
    parse(UpdateOneof::EntryUpdateParent(
        SubscribeUpdateEntryUpdateParent {
            slot,
            cleared_bank_id: bank_id,
            ..Default::default()
        },
    ))
}

#[tokio::test]
async fn recent_hash_survives_repeated_finalization() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(block_meta(10, 100, "recent"));
    storage.push(status(10, SlotStatus::SlotFinalized));
    for slot in 11..=14 {
        storage.push(block_meta(slot, 100 + slot - 10, "latest"));
        storage.push(status(slot, SlotStatus::SlotFinalized));
        storage.push(status(slot, SlotStatus::SlotFinalized));
        assert_eq!(
            storage
                .is_blockhash_valid("recent".to_owned(), CommitmentLevelProto::Finalized)
                .await
                .unwrap(),
            (true, slot)
        );
        assert_eq!(
            storage
                .get_block(CommitmentLevelProto::Finalized)
                .await
                .unwrap()
                .slot,
            slot
        );
    }
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn hash_is_valid_at_last_valid_height_and_expires_after_it() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(block_meta(10, 100, "boundary"));
    storage.push(status(10, SlotStatus::SlotFinalized));
    let last_valid = 100 + MAX_PROCESSING_AGE as u64;
    storage.push(block_meta(11, last_valid, "latest"));
    storage.push(status(11, SlotStatus::SlotFinalized));
    for commitment in [
        CommitmentLevelProto::Processed,
        CommitmentLevelProto::Finalized,
    ] {
        assert_eq!(
            storage
                .is_blockhash_valid("boundary".to_owned(), commitment)
                .await
                .unwrap(),
            (true, 11)
        );
    }
    storage.push(block_meta(12, last_valid + 1, "newer"));
    assert_eq!(
        storage
            .is_blockhash_valid("boundary".to_owned(), CommitmentLevelProto::Processed)
            .await
            .unwrap(),
        (false, 12)
    );
    storage.push(status(12, SlotStatus::SlotFinalized));
    assert_eq!(
        storage
            .is_blockhash_valid("boundary".to_owned(), CommitmentLevelProto::Finalized)
            .await
            .unwrap(),
        (false, 12)
    );
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn confirmed_and_finalized_select_one_bank_of_a_slot() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_meta(10, 300, "winner"));
    storage.push(bank_meta(10, 700, "loser"));
    storage.push(bank_status(10, 300, SlotStatus::SlotConfirmed));
    storage.push(bank_meta(10, 700, "loser"));
    for commitment in [
        CommitmentLevelProto::Processed,
        CommitmentLevelProto::Confirmed,
    ] {
        assert_eq!(
            storage
                .get_block(commitment)
                .await
                .unwrap()
                .blockhash
                .as_str(),
            "winner"
        );
        assert_eq!(
            storage
                .is_blockhash_valid("winner".to_owned(), commitment)
                .await
                .unwrap(),
            (true, 10)
        );
        assert_eq!(
            storage
                .is_blockhash_valid("loser".to_owned(), commitment)
                .await
                .unwrap(),
            (false, 10)
        );
    }
    storage.push(bank_status(10, 300, SlotStatus::SlotFinalized));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Finalized)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "winner"
    );
    assert_eq!(
        storage
            .is_blockhash_valid("loser".to_owned(), CommitmentLevelProto::Finalized)
            .await
            .unwrap(),
        (false, 10)
    );
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn commitment_before_metadata_waits_for_the_selected_bank() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_status(10, 300, SlotStatus::SlotFinalized));
    storage.push(bank_meta(10, 700, "loser"));
    assert!(
        storage
            .get_block(CommitmentLevelProto::Finalized)
            .await
            .is_err()
    );
    storage.push(bank_meta(10, 300, "winner"));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Finalized)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "winner"
    );
    assert_eq!(
        storage
            .is_blockhash_valid("winner".to_owned(), CommitmentLevelProto::Finalized)
            .await
            .unwrap(),
        (true, 10)
    );
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn cleared_bank_cannot_return_through_late_metadata_or_duplicate_markers() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_meta(10, 300, "survivor"));
    storage.push(bank_meta(10, 700, "cleared"));
    storage.push(clear_bank(10, 700));
    storage.push(clear_bank(10, 700));
    storage.push(bank_meta(10, 700, "cleared"));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Processed)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "survivor"
    );
    assert_eq!(
        storage
            .is_blockhash_valid("cleared".to_owned(), CommitmentLevelProto::Processed)
            .await
            .unwrap(),
        (false, 10)
    );
    storage.push(clear_bank(11, 900));
    storage.push(bank_meta(11, 900, "cleared-before-meta"));
    storage.push(bank_meta(11, 1000, "new-bank"));
    assert_eq!(
        storage
            .is_blockhash_valid(
                "cleared-before-meta".to_owned(),
                CommitmentLevelProto::Processed
            )
            .await
            .unwrap(),
        (false, 11)
    );
    storage.push(bank_status(11, 1000, SlotStatus::SlotConfirmed));
    storage.push(clear_bank(11, 1000));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Confirmed)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "new-bank"
    );
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn unbanked_commitment_cannot_guess_between_banked_candidates() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_meta(10, 300, "first"));
    storage.push(bank_meta(10, 700, "second"));
    storage.push(status(10, SlotStatus::SlotConfirmed));
    assert!(
        storage
            .get_block(CommitmentLevelProto::Confirmed)
            .await
            .is_err()
    );
    storage.push(bank_status(10, 300, SlotStatus::SlotFinalized));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Finalized)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "first"
    );
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn explicit_confirmation_resolves_previously_unbanked_confirmation() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_meta(10, 300, "first"));
    storage.push(bank_meta(10, 700, "second"));
    storage.push(status(10, SlotStatus::SlotConfirmed));
    assert!(
        storage
            .get_block(CommitmentLevelProto::Confirmed)
            .await
            .is_err()
    );

    storage.push(bank_status(10, 300, SlotStatus::SlotConfirmed));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Confirmed)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "first"
    );
    for (blockhash, valid) in [("first", true), ("second", false)] {
        assert_eq!(
            storage
                .is_blockhash_valid(blockhash.to_owned(), CommitmentLevelProto::Confirmed)
                .await
                .unwrap(),
            (valid, 10)
        );
    }
    drop(storage);
    worker.await.unwrap();
}

#[tokio::test]
async fn repeated_confirmation_does_not_replace_selected_bank() {
    let (storage, worker) = BlockMetaStorage::new(8);
    storage.push(bank_meta(10, 300, "first"));
    storage.push(bank_meta(10, 700, "second"));
    storage.push(bank_status(10, 300, SlotStatus::SlotConfirmed));
    storage.push(bank_status(10, 700, SlotStatus::SlotConfirmed));
    assert_eq!(
        storage
            .get_block(CommitmentLevelProto::Confirmed)
            .await
            .unwrap()
            .blockhash
            .as_str(),
        "first"
    );
    drop(storage);
    worker.await.unwrap();
}

#[test]
fn finalized_hash_cache_is_bounded_by_processing_age() {
    let mut state = State::default();
    for slot in 1..=1000 {
        state.handle_message(block_meta(slot, slot, &format!("hash-{slot}")));
        state.handle_message(status(slot, SlotStatus::SlotFinalized));
        state.handle_message(status(slot, SlotStatus::SlotFinalized));
        assert!(state.blockhashes.len() <= MAX_PROCESSING_AGE + 1);
    }
    assert_eq!(
        state.is_blockhash_valid("hash-1", CommitmentLevelProto::Finalized),
        Some((false, 1000))
    );
    assert_eq!(
        state.is_blockhash_valid("hash-1000", CommitmentLevelProto::Finalized),
        Some((true, 1000))
    );
}
