use {
    super::*,
    prost::Message as _,
    richat_proto::geyser::{
        SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateEntry,
        SubscribeUpdateSlot, subscribe_update::UpdateOneof,
    },
    std::{hint::black_box, time::Instant},
};

fn channel(max_messages: usize) -> (Messages, Sender) {
    let (mut messages, _) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages,
            max_bytes: 64 * 1024 * 1024,
            storage: None,
        },
        true,
        true,
        true,
        CancellationToken::new(),
    )
    .unwrap();
    let (sender, _) = messages.to_sender(1).unwrap();
    (messages, sender)
}

fn parse(update: UpdateOneof) -> Message {
    Message::parse(
        SubscribeUpdate {
            update_oneof: Some(update),
            created_at: Some(Default::default()),
            ..Default::default()
        }
        .encode_to_vec()
        .into(),
        MessageParserEncoding::Prost,
    )
    .unwrap()
}

fn status(slot: Slot, status: SlotStatus) -> Message {
    parse(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        status: status as i32,
        bank_id: Some(slot),
        ..Default::default()
    }))
}

fn account(index: u64) -> Message {
    let mut pubkey = vec![0; 32];
    pubkey[..8].copy_from_slice(&index.to_le_bytes());
    parse(UpdateOneof::Account(SubscribeUpdateAccount {
        slot: 100,
        bank_id: Some(100),
        account: Some(SubscribeUpdateAccountInfo {
            pubkey,
            owner: vec![2; 32],
            lamports: index,
            write_version: index,
            ..Default::default()
        }),
        ..Default::default()
    }))
}

#[test]
fn slot_only_banks_do_not_allocate_payload_capacity() {
    let (_, mut sender) = channel(128);
    sender
        .push(
            DedupMode::SingleSource,
            "one",
            status(100, SlotStatus::SlotCreatedBank),
        )
        .unwrap();
    let dedup = &sender.dedup[&100].banks[&Some(100)];
    assert_eq!(dedup.accounts_updates.capacity(), 0);
    assert_eq!(dedup.transactions.capacity(), 0);
    assert_eq!(dedup.entries.capacity(), 0);
    let bank = &sender.slots[&100].banks[&Some(100)];
    assert_eq!(bank.messages.capacity(), 0);
}

#[test]
fn first_entry_zero_is_retained_with_lazy_payload_capacity() {
    let (_, mut sender) = channel(128);
    let message = parse(UpdateOneof::Entry(SubscribeUpdateEntry {
        slot: 100,
        bank_id: 100,
        index: 0,
        hash: vec![3; 32],
        ..Default::default()
    }));
    sender
        .push(DedupMode::SingleSource, "one", message.clone())
        .unwrap();
    sender
        .push(DedupMode::SingleSource, "one", message)
        .unwrap();
    let dedup = &sender.dedup[&100].banks[&Some(100)];
    assert!(dedup.entries[0]);
    assert_eq!(dedup.accounts_updates.capacity(), 0);
    assert_eq!(dedup.transactions.capacity(), 0);
    assert_eq!(sender.slots[&100].banks[&Some(100)].entries_count, 1);
}

#[test]
fn single_source_accounts_do_not_allocate_transaction_dedup() {
    let (_, mut sender) = channel(128);
    sender
        .push(DedupMode::SingleSource, "one", account(1))
        .unwrap();
    let dedup = &sender.dedup[&100].banks[&Some(100)];
    assert!(dedup.accounts_updates.capacity() >= 8_192);
    assert_eq!(dedup.transactions.capacity(), 0);
    assert_eq!(dedup.entries.capacity(), 0);
    assert!(sender.slots[&100].banks[&Some(100)].messages.capacity() >= 16_384);
}

#[test]
fn first_sparse_entry_is_retained_at_power_of_two_boundaries() {
    for index in [255, 256, 257, 511, 512, 1_023, 1_024] {
        let (_, mut sender) = channel(128);
        let message = parse(UpdateOneof::Entry(SubscribeUpdateEntry {
            slot: 100,
            bank_id: 100,
            index,
            hash: vec![3; 32],
            ..Default::default()
        }));
        sender
            .push(DedupMode::SingleSource, "one", message.clone())
            .unwrap();
        sender
            .push(DedupMode::SingleSource, "one", message)
            .unwrap();
        assert!(sender.dedup[&100].banks[&Some(100)].entries[index as usize]);
        assert_eq!(sender.slots[&100].banks[&Some(100)].entries_count, 1);
    }
}

#[test]
fn overflowing_entry_index_is_an_explicit_error() {
    let (_, mut sender) = channel(128);
    let message = parse(UpdateOneof::Entry(SubscribeUpdateEntry {
        slot: 100,
        bank_id: 100,
        index: u64::MAX,
        hash: vec![3; 32],
        ..Default::default()
    }));
    assert!(
        sender
            .push(DedupMode::SingleSource, "one", message)
            .is_err()
    );
}

#[test]
fn ownership_is_acquired_after_initial_unbanked_status() {
    let (_, mut sender) = channel(128);
    let mut initial = status(100, SlotStatus::SlotCreatedBank);
    let Message::Slot(MessageSlot::Prost { bank_id, .. }) = &mut initial else {
        panic!("expected slot");
    };
    *bank_id = None;
    sender
        .push(DedupMode::SingleSource, "one", initial)
        .unwrap();
    assert!(sender.slots[&100].source.is_none());
    for index in 1..=8 {
        sender
            .push(DedupMode::SingleSource, "one", account(index))
            .unwrap();
        let source = sender.slots[&100].source.as_ref().unwrap();
        assert_eq!(source.as_ref(), "one");
        assert_eq!(Arc::strong_count(source), 2);
    }
}

#[test]
fn repeated_pending_cycles_retain_small_waiter_capacity() {
    for waiters in [0, 1, 16, 17] {
        let (messages, mut sender) = channel(128);
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        for slot in 1..=8 {
            let mut streams: Vec<_> = (0..waiters)
                .map(|_| messages.subscribe(None, None).unwrap())
                .collect();
            for stream in &mut streams {
                assert!(stream.as_mut().poll_next(&mut context).is_pending());
            }
            sender
                .push(
                    DedupMode::SingleSource,
                    "one",
                    status(slot, SlotStatus::SlotCreatedBank),
                )
                .unwrap();
            for stream in &mut streams {
                assert!(matches!(
                    stream.as_mut().poll_next(&mut context),
                    Poll::Ready(Some(Ok(_)))
                ));
            }
            assert!(sender.processed.shared.wakers_lock().unwrap().capacity() >= 16);
        }
    }
}

#[test]
#[ignore = "local debug timing comparison, not a production performance gate"]
fn forwarding_replay_profile() {
    for scenario in ["slot_only", "unique_accounts", "duplicate_replay"] {
        let templates: Vec<Message> = if scenario == "slot_only" {
            (1..=256)
                .flat_map(|slot| {
                    [
                        status(slot, SlotStatus::SlotCreatedBank),
                        status(slot, SlotStatus::SlotFinalized),
                    ]
                })
                .collect()
        } else {
            (0..8_192).map(account).collect()
        };
        let mut timings = Vec::with_capacity(20);
        for iteration in 0..25 {
            let (messages, mut sender) = channel(32_768);
            if scenario == "duplicate_replay" {
                for message in &templates {
                    sender
                        .push(DedupMode::SingleSource, "one", message.clone())
                        .unwrap();
                }
            }
            let messages = black_box(messages);
            let inputs = templates.clone();
            let started = Instant::now();
            for message in inputs {
                sender
                    .push(DedupMode::SingleSource, "one", message)
                    .unwrap();
            }
            let elapsed = started.elapsed().as_nanos() as f64 / templates.len() as f64;
            if iteration >= 5 {
                timings.push(elapsed);
            }
            if scenario == "slot_only" {
                assert_eq!(sender.slot_finalized, 256);
            } else {
                assert_eq!(
                    sender.dedup[&100].banks[&Some(100)].accounts_updates.len(),
                    8_192
                );
                assert_eq!(sender.slots[&100].banks[&Some(100)].messages.len(), 8_192);
            }
            black_box((messages, sender));
        }
        timings.sort_by(f64::total_cmp);
        eprintln!(
            "profile scenario={scenario} samples={} median_ns_per_input={:.1} min={:.1} max={:.1}",
            timings.len(),
            timings[timings.len() / 2],
            timings[0],
            timings[timings.len() - 1]
        );
    }
}
