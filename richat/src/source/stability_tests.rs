use {
    super::*,
    richat_proto::geyser::{
        GetVersionRequest, GetVersionResponse, SubscribeUpdateBlockFooter,
        SubscribeUpdateEntryUpdateParent, SubscribeUpdatePing,
    },
    richat_shared::transports::grpc::geyser_gen::geyser_server::{Geyser, GeyserServer},
    tokio::{sync::mpsc, task::JoinHandle, time::Instant},
    tonic::{Request, Response, Status, Streaming},
};

struct Batch {
    updates: Vec<(Duration, SubscribeUpdate)>,
    lagged: bool,
}

impl Batch {
    fn empty(lagged: bool) -> Self {
        Self {
            updates: vec![],
            lagged,
        }
    }

    fn data() -> Self {
        Self {
            updates: vec![(Duration::ZERO, slot(100, SlotStatus::SlotProcessed))],
            lagged: false,
        }
    }
}

#[derive(Clone)]
struct TimedUpstream {
    batches: Arc<Mutex<VecDeque<Batch>>>,
    requests: mpsc::UnboundedSender<(Option<Slot>, Instant)>,
    shutdown: CancellationToken,
}

impl Subscribe for TimedUpstream {
    fn subscribe(
        &self,
        from_slot: Option<Slot>,
        filter: Option<RichatFilter>,
    ) -> Result<RecvStream, TransportSubscribeError> {
        assert_eq!(
            filter,
            Some(RichatFilter {
                disable_accounts: false,
                disable_transactions: false,
                disable_entries: false,
            })
        );
        self.requests.send((from_slot, Instant::now())).unwrap();
        let batch = self
            .batches
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra subscription");
        let updates = stream::iter(batch.updates).then(|(delay, message)| async move {
            sleep(delay).await;
            Ok(Arc::new(message.encode_to_vec()))
        });
        let end = if batch.lagged {
            stream::once(async { Err(RecvError::Lagged) }).boxed()
        } else {
            stream::pending().boxed()
        };
        Ok(updates
            .chain(end)
            .take_until(self.shutdown.clone().cancelled_owned())
            .boxed())
    }
}

struct RunningSource {
    requests: mpsc::UnboundedReceiver<(Option<Slot>, Instant)>,
    messages: mpsc::UnboundedReceiver<SubscriptionMessage>,
    source_task: JoinHandle<()>,
    server_task: Option<JoinHandle<Result<(), tokio::task::JoinError>>>,
    shutdown: CancellationToken,
}

impl RunningSource {
    async fn start(batches: Vec<Batch>, reconnect: serde_json::Value) -> Self {
        Self::start_with_parser(batches, reconnect, MessageParserEncoding::Prost).await
    }

    async fn start_with_parser(
        batches: Vec<Batch>,
        reconnect: serde_json::Value,
        parser: MessageParserEncoding,
    ) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let shutdown = CancellationToken::new();
        let (request_tx, requests) = mpsc::unbounded_channel();
        let upstream = TimedUpstream {
            batches: Arc::new(Mutex::new(batches.into())),
            requests: request_tx,
            shutdown: shutdown.clone(),
        };
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let server = GrpcServer::spawn(
            ConfigGrpcServer {
                endpoint: address,
                ..Default::default()
            },
            upstream,
            || {},
            || {},
            VERSION,
            shutdown.clone(),
        )
        .await
        .unwrap();
        let server_task = tokio::spawn(server);
        let mut config = config(format!("http://{address}"), parser);
        let ConfigChannelSource::Grpc {
            general,
            config: client,
            ..
        } = &mut config.sources[0]
        else {
            unreachable!()
        };
        general.reconnect = Some(serde_json::from_value(reconnect).unwrap());
        client.http2_keep_alive_interval = Some(Duration::from_millis(20));
        client.keep_alive_while_idle = true;
        let (mut messages, _) =
            Messages::new(parser, config.config, true, true, true, shutdown.clone()).unwrap();
        let (_, replay) = messages.to_sender(1).unwrap();
        replay.store(100);
        let mut subscriptions = Subscriptions::new(config.sources, replay).await.unwrap();
        let (message_tx, messages) = mpsc::unbounded_channel();
        let source_task = tokio::spawn(async move {
            while let Some(message) = subscriptions.next().await {
                if message_tx.send(message).is_err() {
                    break;
                }
            }
        });
        Self {
            requests,
            messages,
            source_task,
            server_task: Some(server_task),
            shutdown,
        }
    }

    async fn request(&mut self) -> Instant {
        let (from_slot, instant) = timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .expect("source did not reconnect")
            .unwrap();
        assert_eq!(from_slot, Some(100));
        instant
    }

    async fn message(&mut self) {
        let (_, message) = timeout(Duration::from_secs(3), self.messages.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(message, Message::Slot(message) if message.slot() == 100));
    }

    async fn finish(mut self) {
        self.source_task.abort();
        self.shutdown.cancel();
        timeout(Duration::from_secs(3), self.server_task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for RunningSource {
    fn drop(&mut self) {
        self.source_task.abort();
        self.shutdown.cancel();
    }
}

fn reconnect() -> serde_json::Value {
    serde_json::json!({
        "initial_interval": "80ms",
        "max_interval": "320ms",
        "multiplier": 2.0
    })
}

#[test]
fn dragons_mouth_bank_notifications_are_independently_opt_in() {
    for (block_footer, entry_update_parent) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let mut config = serde_json::json!({ "name": "upstream", "parser": "prost" });
        if block_footer {
            config["block_footer"] = true.into();
        }
        if entry_update_parent {
            config["entry_update_parent"] = true.into();
        }
        let general: ConfigChannelSourceGeneral = serde_json::from_value(config).unwrap();
        let request = Subscription::create_dragons_mouth_filter(
            general.disable_accounts,
            general.block_footer,
            general.entry_update_parent,
            Some(100),
        );
        assert_eq!(request.from_slot, Some(100));
        assert_eq!(request.accounts.len(), 1);
        assert_eq!(request.block_footer.len(), usize::from(block_footer));
        if block_footer {
            assert_eq!(request.block_footer[""].include_certificates, Some(true));
        }
        assert_eq!(
            request.entry[""].include_update_parent,
            entry_update_parent.then_some(true)
        );
    }
}

#[test]
fn legacy_source_config_defaults_remain_compatible() {
    let source: ConfigChannelSource = serde_json::from_value(serde_json::json!({
        "transport": "grpc",
        "source": "richat",
        "name": "upstream",
        "parser": "prost",
        "endpoint": "http://127.0.0.1:1",
        "reconnect": {}
    }))
    .unwrap();
    let ConfigChannelSource::Grpc { general, .. } = source else {
        unreachable!()
    };
    assert!(!general.block_footer);
    assert!(!general.entry_update_parent);
    let reconnect = general.reconnect.unwrap();
    assert_eq!(reconnect.idle_timeout, None);
    assert_eq!(reconnect.initial_interval, Duration::from_secs(1));
    assert_eq!(reconnect.max_interval, Duration::from_secs(15));
    assert_eq!(reconnect.multiplier, 2.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_bank_notifications_forward_with_legacy_filter_flags() {
    for parser in [MessageParserEncoding::Prost, MessageParserEncoding::Limited] {
        let expected = vec![
            update(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
                slot: 100,
                bank_id: 1_234_500,
                bank_hash: vec![7; 32],
                ..Default::default()
            })),
            update(UpdateOneof::EntryUpdateParent(
                SubscribeUpdateEntryUpdateParent {
                    slot: 100,
                    cleared_bank_id: 1_234_500,
                    parent_slot: 99,
                    parent_block_id: vec![8; 32],
                },
            )),
        ];
        let mut source = RunningSource::start_with_parser(
            vec![Batch {
                updates: expected
                    .iter()
                    .cloned()
                    .map(|message| (Duration::ZERO, message))
                    .collect(),
                lagged: false,
            }],
            reconnect(),
            parser,
        )
        .await;
        source.request().await;
        for expected in expected {
            let (_, message) = timeout(Duration::from_secs(3), source.messages.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                encoded(&ParsedMessage::from(message)).update_oneof,
                expected.update_oneof,
                "modified native {parser:?} notification"
            );
        }
        source.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_handshakes_then_immediate_lagged_increase_backoff() {
    let mut source = RunningSource::start(
        vec![
            Batch::empty(true),
            Batch::empty(true),
            Batch::empty(true),
            Batch::empty(true),
            Batch::data(),
        ],
        reconnect(),
    )
    .await;
    let first = source.request().await;
    let second = source.request().await;
    let third = source.request().await;
    let fourth = source.request().await;
    let fifth = source.request().await;
    assert!(second.duration_since(first) >= Duration::from_millis(70));
    assert!(third.duration_since(second) >= Duration::from_millis(150));
    assert!(fourth.duration_since(third) >= Duration::from_millis(310));
    assert!(fifth.duration_since(fourth) >= Duration::from_millis(310));
    source.message().await;
    source.finish().await;
}

#[tokio::test(start_paused = true)]
async fn actual_parsed_data_resets_accumulated_backoff() {
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
    let (_, replay) = messages.to_sender(1).unwrap();
    replay.store(100);
    let requests = Arc::new(Mutex::new(vec![]));
    let observed = Arc::clone(&requests);
    let mut source = Subscription::reconnecting_stream(
        "data-reset",
        Backoff::new(serde_json::from_value(reconnect()).unwrap()),
        replay,
        move |requested| {
            assert_eq!(requested, Some(100));
            let mut requests = observed.lock().unwrap();
            let attempt = requests.len();
            requests.push(Instant::now());
            async move {
                let (sender, receiver) = kanal::unbounded_async();
                if attempt >= 2 {
                    let message = Message::parse(
                        slot(100, SlotStatus::SlotProcessed).encode_to_vec().into(),
                        MessageParserEncoding::Prost,
                    )
                    .unwrap();
                    sender.send(Ok(("data-reset", message))).await.unwrap();
                }
                sender
                    .send(Err(richat_client::error::ReceiveError::Lagged.into()))
                    .await
                    .unwrap();
                Ok(receiver)
            }
        },
    );
    for _ in 0..2 {
        assert!(matches!(
            timeout(Duration::from_secs(1), source.next()).await.unwrap(),
            Some(Ok(("data-reset", Message::Slot(message)))) if message.slot() == 100
        ));
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1] - requests[0], Duration::from_millis(80));
    assert_eq!(requests[2] - requests[1], Duration::from_millis(160));
    assert_eq!(requests[3] - requests[2], Duration::from_millis(80));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_errors_do_not_reset_accumulated_backoff() {
    let mut source = RunningSource::start(
        vec![
            Batch::empty(true),
            Batch::empty(true),
            Batch {
                updates: vec![(Duration::ZERO, SubscribeUpdate::default())],
                lagged: false,
            },
            Batch::data(),
        ],
        reconnect(),
    )
    .await;
    source.request().await;
    source.request().await;
    let third = source.request().await;
    let fourth = source.request().await;
    assert!(fourth.duration_since(third) >= Duration::from_millis(310));
    source.message().await;
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_healthy_transport_reconnects_from_same_slot() {
    let mut reconnect = reconnect();
    reconnect["idle_timeout"] = "100ms".into();
    let mut source =
        RunningSource::start(vec![Batch::empty(false), Batch::data()], reconnect).await;
    let first = source.request().await;
    let second = source.request().await;
    assert!(second.duration_since(first) >= Duration::from_millis(170));
    source.message().await;
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_idle_timeout_keeps_silent_stream_open() {
    for explicit_none in [false, true] {
        let mut reconnect = reconnect();
        if explicit_none {
            reconnect["idle_timeout"] = serde_json::Value::Null;
        }
        let mut source = RunningSource::start(vec![Batch::empty(false)], reconnect).await;
        source.request().await;
        assert!(
            timeout(Duration::from_millis(250), source.requests.recv())
                .await
                .is_err()
        );
        assert!(!source.source_task.is_finished());
        source.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pings_refresh_idle_timeout_without_resetting_backoff() {
    let mut reconnect = reconnect();
    reconnect["idle_timeout"] = "100ms".into();
    let pings = Batch {
        updates: (0..4)
            .map(|_| {
                (
                    Duration::from_millis(40),
                    update(UpdateOneof::Ping(SubscribeUpdatePing {})),
                )
            })
            .collect(),
        lagged: false,
    };
    let mut source = RunningSource::start(
        vec![Batch::empty(true), Batch::empty(true), pings, Batch::data()],
        reconnect,
    )
    .await;
    source.request().await;
    source.request().await;
    let third = source.request().await;
    let fourth = source.request().await;
    let delay = fourth.duration_since(third);
    assert!(delay >= Duration::from_millis(560), "delay: {delay:?}");
    source.message().await;
    source.finish().await;
}

#[derive(Clone)]
struct CheckpointUpstream {
    replay: GlobalReplayFromSlot,
    stream_error: bool,
    requests: Arc<Mutex<Vec<(Option<Slot>, Instant)>>>,
    shutdown: CancellationToken,
}

#[tonic::async_trait]
impl Geyser for CheckpointUpstream {
    type SubscribeStream = BoxStream<'static, Result<Arc<Vec<u8>>, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<GrpcSubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner().message().await?.unwrap();
        self.requests
            .lock()
            .unwrap()
            .push((request.replay_from_slot, Instant::now()));
        if request.replay_from_slot == Some(100) {
            let rejected = Status::invalid_argument("first available slot: 101");
            if self.stream_error {
                let replay = self.replay.clone();
                let ping =
                    Arc::new(update(UpdateOneof::Ping(SubscribeUpdatePing {})).encode_to_vec());
                Ok(Response::new(
                    stream::once(async move { Ok(ping) })
                        .chain(stream::once(async move {
                            sleep(Duration::from_millis(20)).await;
                            replay.store(101);
                            Err(rejected)
                        }))
                        .boxed(),
                ))
            } else {
                self.replay.store(101);
                Err(rejected)
            }
        } else {
            let message = Arc::new(slot(101, SlotStatus::SlotProcessed).encode_to_vec());
            Ok(Response::new(
                stream::once(async { Ok(message) })
                    .chain(stream::pending())
                    .take_until(self.shutdown.clone().cancelled_owned())
                    .boxed(),
            ))
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented(
            "native subscriptions do not request a version",
        ))
    }
}

async fn stale_replay_failure_retries_checkpoint(stream_error: bool) {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let shutdown = CancellationToken::new();
    let address = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut config = config(format!("http://{address}"), MessageParserEncoding::Prost);
    let ConfigChannelSource::Grpc { general, .. } = &mut config.sources[0] else {
        unreachable!()
    };
    general.reconnect.as_mut().unwrap().initial_interval = Duration::from_millis(250);
    general.reconnect.as_mut().unwrap().max_interval = Duration::from_millis(500);
    let (mut messages, _) = Messages::new(
        MessageParserEncoding::Prost,
        config.config,
        true,
        true,
        true,
        shutdown.clone(),
    )
    .unwrap();
    let (_, replay) = messages.to_sender(1).unwrap();
    replay.store(100);
    let upstream = CheckpointUpstream {
        replay: replay.clone(),
        stream_error,
        requests: Arc::new(Mutex::new(vec![])),
        shutdown: shutdown.clone(),
    };
    let (incoming, mut builder) = ConfigGrpcServer {
        endpoint: address,
        ..Default::default()
    }
    .create_server_builder()
    .unwrap();
    let server = builder
        .add_service(GeyserServer::new(upstream.clone()))
        .serve_with_incoming_shutdown(incoming, shutdown.clone().cancelled_owned());
    let server_task = tokio::spawn(server);
    let mut subscriptions = Subscriptions::new(config.sources, replay).await.unwrap();
    let received = timeout(Duration::from_secs(3), subscriptions.next()).await;
    drop(subscriptions);
    shutdown.cancel();
    timeout(Duration::from_secs(3), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (_, message) = received.unwrap().unwrap().unwrap();
    assert!(matches!(message, Message::Slot(message) if message.slot() == 101));
    let requests = upstream.requests.lock().unwrap();
    assert_eq!(
        requests.iter().map(|(slot, _)| *slot).collect::<Vec<_>>(),
        [Some(100), Some(101)]
    );
    assert!(requests[1].1.duration_since(requests[0].1) >= Duration::from_millis(240));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_subscribe_replay_failure_retries_advanced_checkpoint() {
    stale_replay_failure_retries_checkpoint(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_stream_replay_failure_retries_advanced_checkpoint() {
    stale_replay_failure_retries_checkpoint(true).await;
}
