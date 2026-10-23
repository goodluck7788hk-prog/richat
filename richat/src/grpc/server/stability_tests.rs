use {
    super::*,
    crate::{channel::Sender, config::ConfigChannelInner},
    futures::{StreamExt, stream},
    http_body_util::StreamBody,
    richat_filter::message::{Message as FilterMessage, MessageParserEncoding},
    richat_proto::geyser::{SlotStatus, SubscribeUpdateSlot},
    tonic::codec::Codec,
};

fn channel(capacity: usize) -> (Messages, Sender) {
    let (mut messages, threads) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages: capacity,
            max_bytes: 1_048_576,
            storage: None,
        },
        false,
        true,
        false,
        CancellationToken::new(),
    )
    .unwrap();
    assert!(threads.is_empty());
    let (sender, _) = messages.to_sender(1).unwrap();
    (messages, sender)
}

fn push_slot(sender: &mut Sender, slot: Slot) {
    let message = FilterMessage::parse(
        Cow::Owned(
            SubscribeUpdate {
                created_at: Some(Default::default()),
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    status: SlotStatus::SlotCreatedBank as i32,
                    ..Default::default()
                })),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        MessageParserEncoding::Prost,
    )
    .unwrap();
    sender.push(false, "cache-test", message).unwrap();
}

#[test]
fn cache_near_zero_retains_messages_after_channel_eviction() {
    let (messages, mut sender) = channel(2);
    let receiver = messages.to_receiver();
    let mut cache = MessagesCache::new(8);
    let head = messages.get_next_index(CommitmentLevel::Processed);
    assert!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head)
            .unwrap()
            .is_none()
    );
    push_slot(&mut sender, 10);
    assert_eq!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head)
            .unwrap()
            .unwrap()
            .slot(),
        10
    );
    for slot in 11..=14 {
        push_slot(&mut sender, slot);
    }
    assert!(matches!(
        receiver.try_recv(CommitmentLevel::Processed, head),
        Err(RecvError::Lagged)
    ));
    let cached = cache
        .try_recv(&receiver, CommitmentLevel::Processed, head)
        .unwrap()
        .unwrap();
    assert!(matches!(cached, Cow::Borrowed(_)));
    assert_eq!(cached.slot(), 10);
}

#[test]
fn cache_eviction_preserves_window_boundary_and_reports_missing_history() {
    let (messages, mut sender) = channel(16);
    let receiver = messages.to_receiver();
    let mut cache = MessagesCache::new(4);
    let head = messages.get_next_index(CommitmentLevel::Processed);
    for offset in 0..8 {
        push_slot(&mut sender, 10 + offset);
        assert_eq!(
            cache
                .try_recv(&receiver, CommitmentLevel::Processed, head + offset)
                .unwrap()
                .unwrap()
                .slot(),
            10 + offset
        );
    }
    assert!(matches!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head + 4)
            .unwrap(),
        Some(Cow::Borrowed(_))
    ));
    assert!(matches!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head + 3)
            .unwrap(),
        Some(Cow::Owned(_))
    ));
    assert!(matches!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head + 7)
            .unwrap(),
        Some(Cow::Borrowed(_))
    ));
    for slot in 18..=40 {
        push_slot(&mut sender, slot);
    }
    assert!(matches!(
        cache
            .try_recv(&receiver, CommitmentLevel::Processed, head)
            .unwrap_err(),
        RecvError::Lagged
    ));
}

fn server() -> GrpcServer {
    let (messages, _) = channel(16);
    GrpcServer {
        shutdown: CancellationToken::new(),
        messages,
        block_meta: None,
        filter_limits: Arc::new(ConfigFilterLimits::default()),
        ping_interval: Duration::from_secs(60),
        subscribe_id: Arc::new(AtomicU64::new(0)),
        subscribe_clients: Arc::new(SegQueue::new()),
        subscribe_messages_len_max: 1_048_576,
        subscribe_messages_replay_len_max: 1_048_576,
    }
}

async fn idle_subscription(server: &GrpcServer) -> (ReceiverStream, std::sync::Weak<()>) {
    let probe = Arc::new(());
    let weak = Arc::downgrade(&probe);
    let body = StreamBody::new(stream::poll_fn(move |_context| {
        let _ = &probe;
        Poll::<Option<Result<hyper::body::Frame<prost::bytes::Bytes>, Status>>>::Pending
    }));
    let decoder = tonic_prost::ProstCodec::<SubscribeUpdate, SubscribeRequest>::default().decoder();
    let request = Request::new(Streaming::new_request(decoder, body, None, None));
    let response = server
        .subscribe2(request, "test", |_| None, |_, _| unreachable!())
        .unwrap()
        .into_inner();
    tokio::task::yield_now().await;
    (response, weak)
}

async fn request_body_released(probe: std::sync::Weak<()>) {
    tokio::time::timeout(Duration::from_millis(100), async {
        while probe.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request reader retained the body after the response finished");
}

#[tokio::test]
async fn dropping_response_cancels_idle_request_reader() {
    let server = server();
    let (response, probe) = idle_subscription(&server).await;
    drop(response);
    request_body_released(probe).await;
    server.shutdown.cancel();
}

#[tokio::test]
async fn marking_client_finished_cancels_idle_request_reader() {
    let server = server();
    let (response, probe) = idle_subscription(&server).await;
    response.client.state_lock().finished = true;
    request_body_released(probe).await;
    server.shutdown.cancel();
}

#[tokio::test]
async fn terminal_response_error_cancels_reader_before_response_drop() {
    let server = server();
    let (mut response, probe) = idle_subscription(&server).await;
    response.client.push_error(Status::data_loss("lagged"));
    assert_eq!(
        response.next().await.unwrap().unwrap_err().code(),
        tonic::Code::DataLoss
    );
    request_body_released(probe).await;
    server.shutdown.cancel();
}
