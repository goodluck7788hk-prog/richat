use {
    super::*,
    futures::task::noop_waker,
    prost::Message as _,
    richat_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
};

#[test]
fn repeated_pending_cycles_retain_small_waiter_capacity() {
    for waiters in [0, 1, 16, 17] {
        let sender = sender(128, 1_048_576);
        for slot in 1..=8 {
            let mut streams: Vec<_> = (0..waiters)
                .map(|_| sender.subscribe(None, None).unwrap())
                .collect();
            for stream in &mut streams {
                assert_eq!(poll_slot(stream), Poll::Pending);
            }
            push_slot(&sender, slot);
            for stream in &mut streams {
                assert_eq!(poll_slot(stream), Poll::Ready(Some(Ok(slot))));
            }
            assert!(sender.shared.state_lock().wakers.capacity() >= 16);
        }
    }
}

fn sender(max_messages: usize, max_bytes: usize) -> Sender {
    Sender::new(
        ConfigChannel {
            max_messages,
            max_bytes,
            ..ConfigChannel::default()
        },
        Arc::new(MaybeRecorder::Noop),
    )
}

fn push_slot(sender: &Sender, slot: Slot) {
    sender.push(
        ProtobufMessage::Slot {
            slot,
            parent: None,
            status: &SlotStatus::Processed,
            bank_id: None,
        },
        ProtobufEncoder::Raw,
    );
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
    let sender = sender(4, 1_048_576);
    let mut stream = sender.subscribe(None, None).unwrap();
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
    for slot in 1..=32 {
        push_slot(&sender, slot);
        assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(slot))));
        assert_eq!(poll_slot(&mut stream), Poll::Pending);
    }
}

#[test]
fn live_subscription_does_not_replay_existing_tail() {
    let sender = sender(4, 1_048_576);
    push_slot(&sender, 1);
    let mut stream = sender.subscribe(None, None).unwrap();
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
    push_slot(&sender, 2);
    assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(2))));
}

#[test]
fn explicit_replay_includes_first_and_latest_message() {
    let sender = sender(4, 1_048_576);
    for slot in 10..=12 {
        push_slot(&sender, slot);
    }
    let mut stream = sender.subscribe(Some(10), None).unwrap();
    for slot in 10..=12 {
        assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(slot))));
    }
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
}

#[test]
fn overwritten_receiver_reports_lagged_instead_of_skipping() {
    let sender = sender(4, 1_048_576);
    push_slot(&sender, 1);
    let mut stream = sender.subscribe(Some(1), None).unwrap();
    for slot in 2..=8 {
        push_slot(&sender, slot);
    }
    assert_eq!(
        poll_slot(&mut stream),
        Poll::Ready(Some(Err(RecvError::Lagged)))
    );
    assert_eq!(poll_slot(&mut stream), Poll::Ready(None));
}

#[test]
fn byte_eviction_reports_lagged_before_ring_wrap() {
    let sender = sender(64, 64);
    push_slot(&sender, 1);
    let mut stream = sender.subscribe(Some(1), None).unwrap();
    for slot in 2..=16 {
        push_slot(&sender, slot);
    }
    assert!(sender.shared.state_lock().tail < 128);
    assert_eq!(
        poll_slot(&mut stream),
        Poll::Ready(Some(Err(RecvError::Lagged)))
    );
}

struct ReentrantWake {
    sender: Sender,
    wakes: std::sync::atomic::AtomicUsize,
}

impl std::task::Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        assert!(self.sender.shared.state.try_lock().is_ok());
        drop(self.sender.subscribe(None, None).unwrap());
        self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn push_wakes_reentrant_receiver_after_unlocking_state() {
    let sender = sender(4, 1_048_576);
    let mut stream = sender.subscribe(None, None).unwrap();
    let wake = Arc::new(ReentrantWake {
        sender: sender.clone(),
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        stream
            .as_mut()
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    push_slot(&sender, 1);
    assert_eq!(wake.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(1))));
}

#[test]
fn close_wakes_reentrant_receiver_after_unlocking_state() {
    let sender = sender(4, 1_048_576);
    let mut stream = sender.subscribe(None, None).unwrap();
    let wake = Arc::new(ReentrantWake {
        sender: sender.clone(),
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        stream
            .as_mut()
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    sender.close();
    assert_eq!(wake.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        poll_slot(&mut stream),
        Poll::Ready(Some(Err(RecvError::Closed)))
    );
}

#[test]
fn ring_eviction_releases_payloads_and_preserves_inclusive_accounting() {
    let sender = sender(4, 1_048_576);
    push_slot(&sender, 1);
    let first = {
        let state = sender.shared.state_lock();
        let item = sender.shared.buffer_idx(sender.shared.get_idx(state.head));
        Arc::downgrade(&item.data.as_ref().unwrap().1)
    };
    for slot in 2..=5 {
        push_slot(&sender, slot);
    }
    assert!(first.upgrade().is_none());
    let state = sender.shared.state_lock();
    assert_eq!(state.tail.wrapping_sub(state.head).wrapping_add(1), 4);
    let stored_bytes: usize = sender
        .shared
        .buffer
        .iter()
        .map(|item| {
            item.lock()
                .unwrap()
                .data
                .as_ref()
                .map_or(0, |(_, data)| data.len())
        })
        .sum();
    assert_eq!(state.bytes_total, stored_bytes);
    assert_eq!(
        state.slots.keys().copied().collect::<Vec<_>>(),
        vec![2, 3, 4, 5]
    );
}

#[test]
fn byte_eviction_releases_payloads_and_keeps_accounting_exact() {
    let sender = sender(64, 80);
    push_slot(&sender, 1);
    let first = {
        let state = sender.shared.state_lock();
        let item = sender.shared.buffer_idx(sender.shared.get_idx(state.head));
        Arc::downgrade(&item.data.as_ref().unwrap().1)
    };
    for slot in 2..=32 {
        push_slot(&sender, slot);
        let state = sender.shared.state_lock();
        let retained: Vec<_> = sender
            .shared
            .buffer
            .iter()
            .filter_map(|item| {
                item.lock()
                    .unwrap()
                    .data
                    .as_ref()
                    .map(|(_, data)| data.len())
            })
            .collect();
        assert_eq!(state.bytes_total, retained.iter().sum::<usize>());
        assert!(state.bytes_total < 80);
        assert_eq!(
            state.tail.wrapping_sub(state.head).wrapping_add(1),
            retained.len() as u64
        );
    }
    assert!(first.upgrade().is_none());
}

#[test]
fn repaired_parent_status_does_not_inherit_child_bank_id() {
    let sender = sender(8, 1_048_576);
    for (slot, parent, status, bank_id) in [
        (9, Some(8), SlotStatus::Processed, 101),
        (10, Some(9), SlotStatus::Processed, 202),
        (10, Some(9), SlotStatus::Confirmed, 202),
    ] {
        sender.push(
            ProtobufMessage::Slot {
                slot,
                parent,
                status: &status,
                bank_id: Some(bank_id),
            },
            ProtobufEncoder::Raw,
        );
    }
    let mut stream = sender.subscribe(Some(9), None).unwrap();
    let waker = noop_waker();
    for (slot, bank_id) in [(9, Some(101)), (10, Some(202)), (9, None), (10, Some(202))] {
        let Poll::Ready(Some(Ok(bytes))) =
            stream.as_mut().poll_next(&mut Context::from_waker(&waker))
        else {
            panic!("expected slot")
        };
        let Some(UpdateOneof::Slot(update)) = SubscribeUpdate::decode(bytes.as_slice())
            .unwrap()
            .update_oneof
        else {
            panic!("expected slot")
        };
        assert_eq!((update.slot, update.bank_id), (slot, bank_id));
    }
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
}

#[test]
fn entry_filter_also_suppresses_parent_markers() {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaEntryInfoV2, ReplicaEntryUpdateParentInfo,
        },
        solana_hash::Hash,
    };

    let sender = sender(8, 1_048_576);
    let mut stream = sender
        .subscribe(
            None,
            Some(RichatFilter {
                disable_entries: true,
                ..RichatFilter::default()
            }),
        )
        .unwrap();
    let parent_hash = Hash::new_from_array([9; 32]);
    let parent = ReplicaEntryUpdateParentInfo {
        slot: 10,
        cleared_bank_id: 16_385,
        parent_slot: 9,
        parent_block_id: &parent_hash,
    };
    sender.push(
        ProtobufMessage::EntryUpdateParent {
            update_parent: &parent,
        },
        ProtobufEncoder::Raw,
    );
    let entry = ReplicaEntryInfoV2 {
        slot: 10,
        index: 0,
        num_hashes: 1,
        hash: &[0; 32],
        executed_transaction_count: 0,
        starting_transaction_index: 0,
    };
    sender.push(
        ProtobufMessage::Entry {
            entry: &entry,
            bank_id: 16_385,
        },
        ProtobufEncoder::Raw,
    );
    push_slot(&sender, 10);
    assert_eq!(poll_slot(&mut stream), Poll::Ready(Some(Ok(10))));
    assert_eq!(poll_slot(&mut stream), Poll::Pending);
}
