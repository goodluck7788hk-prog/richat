use {
    super::*,
    futures::task::noop_waker,
    prost::Message as _,
    richat_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
    std::sync::Barrier,
};

fn channel(max_messages: usize, max_bytes: usize) -> (Messages, Sender) {
    let (mut messages, threads) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages,
            max_bytes,
            storage: None,
        },
        true,
        true,
        true,
        CancellationToken::new(),
    )
    .unwrap();
    assert!(threads.is_empty());
    let (sender, _) = messages.to_sender(1).unwrap();
    (messages, sender)
}

fn push_slot(sender: &mut Sender, slot: Slot, status: SlotStatus) {
    sender
        .push(
            false,
            "test",
            Message::Slot(MessageSlot::Prost {
                slot,
                parent: None,
                status,
                dead_error: None,
                bank_id: None,
                created_at: Default::default(),
                size: 32,
            }),
        )
        .unwrap();
}

fn poll_slot(stream: &mut RecvStream) -> Poll<Option<Result<Slot, RecvError>>> {
    let waker = noop_waker();
    stream
        .as_mut()
        .poll_next(&mut Context::from_waker(&waker))
        .map(|message| {
            message.map(|message| {
                message.map(|bytes| {
                    let update = SubscribeUpdate::decode(bytes.as_slice()).unwrap();
                    match update.update_oneof.unwrap() {
                        UpdateOneof::Slot(message) => message.slot,
                        _ => panic!("expected slot"),
                    }
                })
            })
        })
}

#[test]
fn live_subscription_before_first_message_survives_ring_wrap() {
    let (messages, mut sender) = channel(4, 1_048_576);
    let mut stream = messages.subscribe(None, None).unwrap();
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
    for slot in 1..=32 {
        push_slot(&mut sender, slot, SlotStatus::SlotCreatedBank);
        assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(slot))));
        assert_eq!(poll_slot(&mut stream), Poll::Pending);
    }
}

#[test]
fn live_subscription_does_not_replay_existing_tail() {
    let (messages, mut sender) = channel(4, 1_048_576);
    push_slot(&mut sender, 1, SlotStatus::SlotCreatedBank);
    let mut stream = messages.subscribe(None, None).unwrap();
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
    push_slot(&mut sender, 2, SlotStatus::SlotCreatedBank);
    assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(2))));
}

#[test]
fn explicit_replay_includes_first_and_latest_message() {
    let (messages, mut sender) = channel(4, 1_048_576);
    for slot in 10..=12 {
        push_slot(&mut sender, slot, SlotStatus::SlotCreatedBank);
    }
    let mut stream = messages.subscribe(Some(10), None).unwrap();
    for slot in 10..=12 {
        assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(slot))));
    }
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
}

#[test]
fn sync_receiver_reads_the_published_tail_at_every_commitment() {
    let (messages, mut sender) = channel(8, 1_048_576);
    let receiver = messages.to_receiver();
    let head = messages.get_current_tail(CommitmentLevel::Processed) + 1;
    push_slot(&mut sender, 1, SlotStatus::SlotFinalized);
    for commitment in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        let message = receiver.try_recv(commitment, head).unwrap();
        assert!(matches!(message, Some(ParsedMessage::Slot(message)) if message.slot() == 1));
        assert!(receiver.try_recv(commitment, head + 1).unwrap().is_none());
    }
}

#[test]
fn live_grpc_head_waits_for_the_next_message_even_when_empty() {
    let (messages, mut sender) = channel(8, 1_048_576);
    let receiver = messages.to_receiver();
    let IndexLocation::Memory(head) = messages
        .get_current_tail_with_replay(CommitmentLevel::Processed, None)
        .unwrap()
    else {
        panic!("expected memory cursor");
    };
    assert!(
        receiver
            .try_recv(CommitmentLevel::Processed, head)
            .unwrap()
            .is_none()
    );
    push_slot(&mut sender, 1, SlotStatus::SlotCreatedBank);
    assert!(
        receiver
            .try_recv(CommitmentLevel::Processed, head)
            .unwrap()
            .is_some()
    );
}

#[test]
fn publication_between_empty_check_and_wait_registration_is_not_lost() {
    let (messages, mut sender) = channel(8, 1_048_576);
    let ready = Arc::new(Barrier::new(2));
    let published = Arc::new(Barrier::new(2));
    let mut receiver = ReceiverAsync {
        shared: Arc::clone(&messages.shared_processed),
        head: messages.get_current_tail(CommitmentLevel::Processed) + 1,
        finished: false,
        enable_notifications_accounts: true,
        enable_notifications_transactions: true,
        enable_notifications_entries: true,
        before_wait: Some((Arc::clone(&ready), Arc::clone(&published))),
    };
    let producer = std::thread::spawn(move || {
        ready.wait();
        push_slot(&mut sender, 1, SlotStatus::SlotCreatedBank);
        published.wait();
    });
    let result = receiver.recv_ref(&noop_waker());
    producer.join().unwrap();
    assert!(matches!(result, Ok(Some(_))));
}

#[test]
fn overwritten_receiver_reports_lagged_instead_of_skipping() {
    let (messages, mut sender) = channel(4, 1_048_576);
    push_slot(&mut sender, 1, SlotStatus::SlotCreatedBank);
    let mut stream = messages.subscribe(Some(1), None).unwrap();
    for slot in 2..=8 {
        push_slot(&mut sender, slot, SlotStatus::SlotCreatedBank);
    }
    assert_eq!(
        poll_slot(&mut stream),
        Poll::Ready(Some(Err(RecvError::Lagged)))
    );
    assert_eq!(poll_slot(&mut stream), Poll::Ready(None));
}

#[test]
fn first_processed_notification_does_not_underflow_the_empty_channel_count() {
    let (_, mut sender) = channel(4, 1_048_576);
    push_slot(&mut sender, 1, SlotStatus::SlotProcessed);
}

#[test]
fn replay_failure_votes_expire_when_the_checkpoint_advances() {
    let replay = GlobalReplayFromSlot::new(Some(100), 2);
    assert!(!replay.report_replay_failed("one"));
    replay.store(101);
    assert!(!replay.report_replay_failed_at("one", Some(100)));
    assert!(!replay.report_replay_failed("two"));
    assert!(replay.report_replay_failed("one"));
}

#[test]
fn daemon_wakes_receivers_without_holding_channel_or_replay_locks() {
    struct CheckWake {
        shared: Arc<SharedChannel>,
        replay: Arc<Mutex<BTreeMap<Slot, ReplayInfo>>>,
    }
    impl std::task::Wake for CheckWake {
        fn wake(self: Arc<Self>) {
            assert!(self.shared.wakers.as_ref().unwrap().try_lock().is_ok());
            assert!(self.replay.try_lock().is_ok());
        }
    }
    let (messages, mut sender) = channel(8, 1_048_576);
    let waker = Waker::from(Arc::new(CheckWake {
        shared: Arc::clone(&messages.shared_processed),
        replay: messages.replay_info.as_ref().unwrap().clone(),
    }));
    let mut stream = messages.subscribe(None, None).unwrap();
    assert!(
        stream
            .as_mut()
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    push_slot(&mut sender, 1, SlotStatus::SlotCreatedBank);
}

#[test]
fn recovered_partial_slot_replay_starts_from_disk_not_only_its_new_tail() {
    let (messages, mut sender) = channel(8, 1_048_576);
    sender.processed.incomplete_slots.insert(100);
    push_slot(&mut sender, 100, SlotStatus::SlotCreatedBank);
    let replay = messages.replay_info.as_ref().unwrap();
    mutex_lock(replay).insert(
        100,
        ReplayInfo {
            head: 42,
            messages: Some(IntSet::default()),
        },
    );
    assert!(matches!(
        messages
            .get_current_tail_with_replay(CommitmentLevel::Processed, Some(100))
            .unwrap(),
        IndexLocation::Storage(42)
    ));
    assert!(messages.subscribe(Some(100), None).is_err());
    sender.storage_max_slots = 1;
    push_slot(&mut sender, 100, SlotStatus::SlotFinalized);
    push_slot(&mut sender, 101, SlotStatus::SlotCreatedBank);
    push_slot(&mut sender, 101, SlotStatus::SlotFinalized);
    assert!(!mutex_lock(replay).contains_key(&100));
    assert!(messages.shared_processed.slots_lock().contains_key(&100));
    assert!(
        messages
            .get_current_tail_with_replay(CommitmentLevel::Processed, Some(100))
            .is_err()
    );
    assert!(messages.subscribe(Some(100), None).is_err());
}
