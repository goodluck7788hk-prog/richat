use {super::*, futures::StreamExt, tonic::transport::server::TcpConnectInfo};

fn request_with_peer(peer: Option<&str>, client_name: &str) -> Request<()> {
    let mut request = Request::new(());
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: peer.map(|peer| peer.parse().unwrap()),
    });
    request
        .metadata_mut()
        .insert(X_SUBSCRIPTION_ID, client_name.parse().unwrap());
    request
}

fn render(server: &GrpcServer) -> String {
    let mut output = String::new();
    server.backlog_metrics.append_to(&mut output);
    output
}

fn sample(output: &str, metric: &str, subscription_id: u64) -> f64 {
    let id = format!("subscription_id=\"{subscription_id}\"");
    output
        .lines()
        .find(|line| line.starts_with(&format!("{metric}{{")) && line.contains(&id))
        .unwrap_or_else(|| panic!("missing {metric} for {subscription_id}: {output}"))
        .rsplit_once(' ')
        .unwrap()
        .1
        .parse()
        .unwrap()
}

fn register(
    server: &GrpcServer,
    id: u64,
    peer: Option<&str>,
    name: &str,
) -> (SubscribeClient, backlog::Registration) {
    let client = SubscribeClient::new(id, 1_024, 1_024, Arc::from(name));
    let registration = server
        .backlog_metrics
        .register(id, &request_with_peer(peer, name), &client);
    (client, registration)
}

fn memory_cursor(server: &GrpcServer, client: &SubscribeClient, commitment: CommitmentLevel) {
    let mut state = client.state_lock();
    state.filter = Some(Filter::new(&ConfigFilter::default()));
    state.commitment = commitment;
    state.head = IndexLocation::Memory(server.messages.get_next_index(commitment));
}

#[test]
fn backlog_distinguishes_same_ip_and_blank_or_duplicate_names() {
    let server = stability_tests::server();
    let (_first, _first_registration) = register(&server, 1, Some("192.0.2.1:1234"), "");
    let (_second, _second_registration) = register(&server, 2, Some("192.0.2.1:5678"), "");
    let (_third, _third_registration) = register(&server, 3, Some("192.0.2.2:1234"), "same");
    let (_fourth, _fourth_registration) = register(&server, 4, Some("192.0.2.2:5678"), "same");
    let output = render(&server);
    for id in 1..=4 {
        assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", id), 0.0);
    }
    assert!(output.contains("source_ip=\"192.0.2.1\""));
    assert!(output.contains("source_ip=\"192.0.2.2\""));
    assert!(output.contains("x_subscription_id=\"\""));
}

#[test]
fn backlog_uses_transport_peer_not_forwarded_headers() {
    let server = stability_tests::server();
    let client = SubscribeClient::new(1, 1_024, 1_024, Arc::from(""));
    let mut request = request_with_peer(Some("192.0.2.1:1234"), "");
    request
        .metadata_mut()
        .insert("x-forwarded-for", "198.51.100.2".parse().unwrap());
    let _registration = server.backlog_metrics.register(1, &request, &client);
    let output = render(&server);
    assert!(output.contains("source_ip=\"192.0.2.1\""));
    assert!(!output.contains("198.51.100.2"));
}

#[test]
fn backlog_normalizes_ipv4_mapped_peers_and_marks_missing_peers() {
    let server = stability_tests::server();
    let (_first, _first_registration) = register(&server, 1, Some("[::ffff:192.0.2.1]:1234"), "");
    let (_second, _second_registration) = register(&server, 2, Some("[2001:db8::1]:1234"), "");
    let (_third, _third_registration) = register(&server, 3, None, "");
    let output = render(&server);
    assert!(output.contains("source_ip=\"192.0.2.1\""));
    assert!(output.contains("source_ip=\"2001:db8::1\""));
    assert!(output.contains("source_ip=\"unknown\""));
}

#[test]
fn backlog_escapes_and_bounds_optional_client_labels() {
    let server = stability_tests::server();
    let name = format!("name\\\"{}", "a".repeat(1_024));
    let (_client, _registration) = register(&server, 1, Some("192.0.2.1:1234"), &name);
    let output = render(&server);
    assert!(output.contains("x_subscription_id=\"name\\\\\\\""));
    assert!(!output.contains(&"a".repeat(129)));
}

#[test]
fn backlog_pending_bytes_follow_push_dequeue_and_terminal_drain() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    client.push_message(GrpcSubscribeMessage::Ping, vec![0; 19]);
    client.push_message(GrpcSubscribeMessage::Pong, vec![0; 23]);
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_pending_bytes", 1),
        42.0
    );
    let mut response = ReceiverStream::new(client.clone());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(Pin::new(&mut response).poll_next(&mut context).is_ready());
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_pending_bytes", 1),
        23.0
    );
    client.push_error(Status::data_loss("lagged"));
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_pending_bytes", 1),
        0.0
    );
}

#[test]
fn backlog_memory_lag_advances_without_pending_output() {
    let (messages, mut sender) = stability_tests::channel(2);
    let mut server = stability_tests::server();
    server.messages = messages.clone();
    server.backlog_metrics = backlog::BacklogMetrics::new(messages);
    let (client, _registration) = register(&server, 1, None, "");
    memory_cursor(&server, &client, CommitmentLevel::Processed);
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        0.0
    );
    for slot in 10..=14 {
        stability_tests::push_slot(&mut sender, slot);
    }
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        5.0
    );
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_pending_bytes", 1),
        0.0
    );
    client.state_lock().head =
        IndexLocation::Memory(server.messages.get_next_index(CommitmentLevel::Processed));
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        0.0
    );
}

#[test]
fn backlog_lag_uses_each_readers_commitment_channel() {
    let (messages, mut sender) = stability_tests::channel(16);
    let mut server = stability_tests::server();
    server.messages = messages.clone();
    server.backlog_metrics = backlog::BacklogMetrics::new(messages);
    let (processed, _processed_registration) = register(&server, 1, None, "");
    let (confirmed, _confirmed_registration) = register(&server, 2, None, "");
    let (finalized, _finalized_registration) = register(&server, 3, None, "");
    memory_cursor(&server, &processed, CommitmentLevel::Processed);
    memory_cursor(&server, &confirmed, CommitmentLevel::Confirmed);
    memory_cursor(&server, &finalized, CommitmentLevel::Finalized);
    let message = richat_filter::message::Message::parse(
        Cow::Owned(
            SubscribeUpdate {
                created_at: Some(Default::default()),
                update_oneof: Some(UpdateOneof::Account(
                    richat_proto::geyser::SubscribeUpdateAccount {
                        slot: 10,
                        account: Some(richat_proto::geyser::SubscribeUpdateAccountInfo {
                            pubkey: vec![1; 32],
                            owner: vec![2; 32],
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        richat_filter::message::MessageParserEncoding::Prost,
    )
    .unwrap();
    sender.push(false, "cache-test", message).unwrap();
    let output = render(&server);
    assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 1), 1.0);
    assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 2), 0.0);
    assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 3), 0.0);
}

#[test]
fn backlog_cursor_ahead_of_channel_is_unknown_not_zero() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    memory_cursor(&server, &client, CommitmentLevel::Processed);
    client.state_lock().head = IndexLocation::Memory(u64::MAX);
    let output = render(&server);
    assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 1), -1.0);
    assert_eq!(sample(&output, "grpc_subscribe_backlog_available", 1), 0.0);
}

#[test]
fn backlog_unknown_and_disk_cursors_do_not_report_false_zero() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        -1.0
    );
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_backlog_available", 1),
        0.0
    );
    assert!(render(&server).contains("state=\"unset\""));
    client.state_lock().head = IndexLocation::Storage(999_999);
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        -1.0
    );
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_replay_active", 1),
        1.0
    );
    assert!(render(&server).contains("state=\"disk\""));
    memory_cursor(&server, &client, CommitmentLevel::Processed);
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_lag_messages", 1),
        0.0
    );
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_backlog_available", 1),
        1.0
    );
    assert!(render(&server).contains("state=\"memory\""));
}

#[test]
fn backlog_scrape_does_not_wait_for_client_state_lock() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    let state = client.state_lock();
    let sampler = server.backlog_metrics.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut output = String::new();
        sampler.append_to(&mut output);
        sender.send(output).unwrap();
    });
    let result = receiver.recv_timeout(Duration::from_secs(1));
    drop(state);
    thread.join().unwrap();
    let output = result.expect("metrics scrape waited for a hot worker state lock");
    assert_eq!(sample(&output, "grpc_subscribe_backlog_available", 1), 0.0);
    assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 1), -1.0);
    assert_eq!(sample(&output, "grpc_subscribe_replay_active", 1), -1.0);
    assert!(output.contains("state=\"busy\""));
}

#[test]
fn backlog_busy_state_still_exports_queue_bytes_and_recovers_next_scrape() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    memory_cursor(&server, &client, CommitmentLevel::Processed);
    client.push_message(GrpcSubscribeMessage::Ping, vec![0; 19]);
    let state = client.state_lock();
    let output = render(&server);
    assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 1), 19.0);
    assert_eq!(sample(&output, "grpc_subscribe_backlog_available", 1), 0.0);
    drop(state);
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_backlog_available", 1),
        1.0
    );
}

#[test]
fn backlog_dropped_registration_does_not_leave_old_series() {
    let server = stability_tests::server();
    let (_client, registration) = register(&server, 1, None, "");
    assert!(render(&server).contains("subscription_id=\"1\""));
    drop(registration);
    assert!(!render(&server).contains("subscription_id=\"1\""));
    assert!(server.backlog_metrics.is_empty());
}

#[test]
fn backlog_scrapes_do_not_mutate_queue_or_cursor() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    memory_cursor(&server, &client, CommitmentLevel::Processed);
    client.push_message(GrpcSubscribeMessage::Ping, vec![0; 19]);
    let head = server.messages.get_next_index(CommitmentLevel::Processed);
    for _ in 0..100 {
        let output = render(&server);
        assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 1), 19.0);
        assert_eq!(sample(&output, "grpc_subscribe_lag_messages", 1), 0.0);
    }
    assert_eq!(client.messages_len.load(Ordering::Relaxed), 19);
    assert_eq!(client.messages.len(), 1);
    assert!(matches!(client.state_lock().head, IndexLocation::Memory(cursor) if cursor == head));
}

#[test]
fn backlog_keeps_registration_and_rendering_out_of_delivery_loops() {
    let source = include_str!("../server.rs");
    let worker = source
        .split_once("    fn worker_messages(")
        .unwrap()
        .1
        .split_once("    fn subscribe2")
        .unwrap()
        .0;
    let push = source
        .split_once("    pub fn push_message(")
        .unwrap()
        .1
        .split_once("    pub fn push_error(")
        .unwrap()
        .0;
    let poll = source
        .split_once("    fn poll_next(")
        .unwrap()
        .1
        .split_once("self.finished = item.is_err();")
        .unwrap()
        .0;
    for hot_path in [worker, push, poll] {
        assert!(!hot_path.contains("backlog"));
        assert!(!hot_path.contains("source_ip"));
        assert!(!hot_path.contains("append_to"));
    }
    assert_eq!(push.matches("fetch_add").count(), 1);
    assert_eq!(poll.matches("fetch_sub").count(), 1);
}

#[test]
fn backlog_export_preserves_existing_metrics_and_drops_closed_series() {
    let server = stability_tests::server();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    ::metrics::with_local_recorder(&recorder, || {
        ::metrics::gauge!("backlog_test_existing_metric").set(42.0);
    });
    let (_client, registration) = register(&server, 1, None, "");
    let output =
        String::from_utf8(crate::metrics::render(&handle, &server.backlog_metrics)).unwrap();
    assert!(output.contains("backlog_test_existing_metric 42"));
    assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 1), 0.0);
    drop(registration);
    let output =
        String::from_utf8(crate::metrics::render(&handle, &server.backlog_metrics)).unwrap();
    assert!(output.contains("backlog_test_existing_metric 42"));
    assert!(!output.contains("subscription_id="));
}

#[test]
fn backlog_exposition_groups_each_typed_family_with_its_samples() {
    let server = stability_tests::server();
    let (_first, _first_registration) = register(&server, 1, Some("192.0.2.1:1234"), "name\\\"");
    let (_second, _second_registration) = register(&server, 2, Some("192.0.2.2:1234"), "");
    let output = render(&server);
    let mut previous = "";
    let mut seen = HashSet::new();
    for line in output.lines().filter(|line| !line.is_empty()) {
        let family = if line.starts_with('#') {
            line.split_whitespace().nth(2).unwrap()
        } else {
            line.split_once('{').unwrap().0
        };
        if previous != family {
            assert!(seen.insert(family), "non-contiguous metric family {family}");
            previous = family;
        }
    }
    for family in seen {
        assert_eq!(
            output.matches(&format!("# TYPE {family} gauge\n")).count(),
            1
        );
        assert_eq!(
            output
                .lines()
                .filter(|line| line.starts_with(&format!("{family}{{")))
                .count(),
            2
        );
    }
    assert!(output.contains("x_subscription_id=\"name\\\\\\\"\""));
}

#[tokio::test]
async fn backlog_subscribe_accounts_uses_the_same_registration_lifecycle() {
    use {
        super::geyser_gen::geyser_server::Geyser, http_body_util::StreamBody, tonic::codec::Codec,
    };
    let server = stability_tests::server();
    let body = StreamBody::new(futures::stream::pending::<
        Result<hyper::body::Frame<prost::bytes::Bytes>, Status>,
    >());
    let decoder =
        tonic_prost::ProstCodec::<SubscribeUpdate, SubscribeAccountsRequest>::default().decoder();
    let mut request = Request::new(Streaming::new_request(decoder, body, None, None));
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some("192.0.2.1:1234".parse().unwrap()),
    });
    let response = server.subscribe_accounts(request).await.unwrap();
    let output = render(&server);
    assert!(output.contains("source_ip=\"192.0.2.1\""));
    assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 0), 0.0);
    drop(response);
    assert!(server.backlog_metrics.is_empty());
    server.shutdown.cancel();
}

#[tokio::test]
async fn backlog_tcp_peer_and_half_closed_streams_keep_distinct_registrations() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (messages, _) = stability_tests::channel(16);
    let backlog = backlog::BacklogMetrics::new(messages.clone());
    let shutdown = CancellationToken::new();
    let _shutdown_guard = shutdown.clone().drop_guard();
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut config = ConfigAppsGrpc::default();
    config.server.endpoint = address;
    config.unary.enabled = false;
    config.workers.messages_cached_max = 16;
    let task = tokio::spawn(
        GrpcServer::spawn_with_backlog_metrics(config, messages, shutdown.clone(), backlog.clone())
            .unwrap(),
    );
    let mut client = richat_client::grpc::ConfigGrpcClient {
        endpoint: format!("http://{address}"),
        ..Default::default()
    }
    .connect()
    .await
    .unwrap();
    let first = client
        .subscribe_dragons_mouth_once(SubscribeRequest::default())
        .await
        .unwrap();
    let second = client
        .subscribe_dragons_mouth_once(SubscribeRequest::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut output = String::new();
            backlog.append_to(&mut output);
            if output.matches("state=\"memory\"").count() == 2 {
                assert!(output.contains("source_ip=\"127.0.0.1\""));
                assert!(!output.contains("source_ip=\"unknown\""));
                assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 0), 0.0);
                assert_eq!(sample(&output, "grpc_subscribe_pending_bytes", 1), 0.0);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(first);
    drop(second);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !backlog.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn backlog_registration_cleanup_and_weak_ownership() {
    let server = stability_tests::server();
    for id in 0..1_000 {
        let (client, registration) = register(&server, id, None, "");
        let state = Arc::downgrade(&client.state);
        drop(client);
        assert!(state.upgrade().is_none());
        assert!(!render(&server).contains("source_ip="));
        drop(registration);
    }
    assert!(server.backlog_metrics.is_empty());
}

#[test]
fn backlog_finished_subscriptions_disappear_even_if_clones_survive() {
    let server = stability_tests::server();
    let (client, _registration) = register(&server, 1, None, "");
    client.state_lock().finished = true;
    assert!(!render(&server).contains("source_ip="));
}

#[tokio::test]
async fn backlog_response_drop_removes_registration_before_worker_cleanup() {
    let server = stability_tests::server();
    let (response, probe) = stability_tests::idle_subscription(&server).await;
    assert_eq!(
        sample(&render(&server), "grpc_subscribe_pending_bytes", 0),
        0.0
    );
    drop(response);
    assert!(server.backlog_metrics.is_empty());
    stability_tests::request_body_released(probe).await;
    server.shutdown.cancel();
}

#[tokio::test]
async fn backlog_terminal_response_error_removes_registration() {
    let server = stability_tests::server();
    let (mut response, probe) = stability_tests::idle_subscription(&server).await;
    response.client.push_error(Status::data_loss("lagged"));
    assert!(response.next().await.unwrap().is_err());
    assert!(server.backlog_metrics.is_empty());
    stability_tests::request_body_released(probe).await;
    server.shutdown.cancel();
}
