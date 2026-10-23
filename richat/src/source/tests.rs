use {
    super::*,
    crate::{
        channel::{DedupMode, Messages, ParsedMessage},
        config::{ConfigChannel, ConfigChannelInner},
        grpc::{config::ConfigAppsGrpc, server::GrpcServer as FanoutServer},
        version::VERSION,
    },
    futures::stream,
    prost::Message as _,
    richat_filter::{filter::FilteredUpdate, message::MessageParserEncoding},
    richat_proto::{
        geyser::{
            SlotStatus, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            subscribe_update::UpdateOneof,
        },
        solana::storage::confirmed_block::{
            BlockHeight, Message as TransactionMessage, MessageHeader, Transaction,
            TransactionConfig, TransactionStatusMeta,
        },
    },
    richat_shared::transports::{
        RecvError, RecvStream, Subscribe, SubscribeError as TransportSubscribeError,
        grpc::{ConfigGrpcServer, GrpcServer},
    },
    solana_commitment_config::CommitmentLevel,
    std::{collections::VecDeque, net::TcpListener, sync::Arc},
    tokio::time::timeout,
    tokio_util::sync::CancellationToken,
};

#[path = "stability_tests.rs"]
mod stability;

#[derive(Clone)]
struct ReplayUpstream {
    batches: Arc<Mutex<VecDeque<Vec<SubscribeUpdate>>>>,
    requests: Arc<Mutex<Vec<Option<Slot>>>>,
    reject_replay: bool,
    shutdown: CancellationToken,
}

impl Subscribe for ReplayUpstream {
    fn subscribe(
        &self,
        from_slot: Option<Slot>,
        _filter: Option<RichatFilter>,
    ) -> Result<RecvStream, TransportSubscribeError> {
        self.requests.lock().unwrap().push(from_slot);
        if self.reject_replay && from_slot.is_some() {
            return Err(TransportSubscribeError::SlotNotAvailable {
                first_available: 101,
            });
        }
        let mut batches = self.batches.lock().unwrap();
        let batch = batches.pop_front().expect("unexpected extra subscription");
        let final_batch = batches.is_empty();
        let messages = stream::iter(
            batch
                .into_iter()
                .map(|message| Ok(Arc::new(message.encode_to_vec()))),
        );
        if final_batch && !self.reject_replay {
            Ok(messages
                .chain(stream::pending())
                .take_until(self.shutdown.clone().cancelled_owned())
                .boxed())
        } else {
            Ok(messages
                .chain(stream::once(async { Err(RecvError::Lagged) }))
                .boxed())
        }
    }
}

fn config(endpoint: String, parser: MessageParserEncoding) -> ConfigChannel {
    ConfigChannel {
        tokio: Default::default(),
        sources_sighup_reload: false,
        sources: vec![ConfigChannelSource::Grpc {
            general: ConfigChannelSourceGeneral {
                name: "reconnecting-test".to_owned(),
                parser,
                disable_accounts: false,
                block_footer: false,
                entry_update_parent: false,
                exclude_on_finish: false,
                reconnect: Some(ConfigChannelSourceReconnect {
                    idle_timeout: None,
                    initial_interval: Duration::from_millis(1),
                    max_interval: Duration::from_millis(4),
                    multiplier: 2.0,
                }),
                channel_size: 1,
            },
            source: ConfigGrpcClientSource::Richat,
            config: ConfigGrpcClient {
                endpoint,
                ..Default::default()
            },
        }],
        config: ConfigChannelInner {
            max_messages: 128,
            max_bytes: 1_048_576,
            storage: None,
        },
    }
}

fn update(message: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        created_at: Some(Default::default()),
        update_oneof: Some(message),
        ..Default::default()
    }
}

fn slot(slot: Slot, status: SlotStatus) -> SubscribeUpdate {
    update(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        status: status as i32,
        ..Default::default()
    }))
}

fn signature(slot: Slot, index: u64) -> Vec<u8> {
    vec![((slot % 32) * 4 + index + 1) as u8; 64]
}

fn account(slot: Slot, index: u64, phantom: bool) -> SubscribeUpdate {
    update(UpdateOneof::Account(SubscribeUpdateAccount {
        slot,
        account: Some(SubscribeUpdateAccountInfo {
            pubkey: vec![
                if phantom {
                    3
                } else if index < 2 {
                    1
                } else {
                    2
                };
                32
            ],
            owner: vec![9; 32],
            lamports: slot + (index + 1) * 10,
            data: vec![index as u8; 8],
            write_version: slot * 100 + index,
            txn_signature: (!phantom).then(|| signature(slot, index)),
            ..Default::default()
        }),
        ..Default::default()
    }))
}

fn transaction(slot: Slot, index: u64) -> SubscribeUpdate {
    update(UpdateOneof::Transaction(SubscribeUpdateTransaction {
        slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: signature(slot, index),
            index,
            transaction: Some(Transaction {
                signatures: vec![signature(slot, index)],
                message: Some(TransactionMessage {
                    header: Some(MessageHeader {
                        num_required_signatures: 1,
                        ..Default::default()
                    }),
                    account_keys: vec![vec![1; 32]],
                    recent_blockhash: vec![4; 32],
                    versioned: index > 0,
                    config: (index == 2).then_some(TransactionConfig {
                        priority_fee: Some(5_000),
                        compute_unit_limit: Some(70_000),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }),
            meta: Some(TransactionStatusMeta {
                fee: 5_000,
                compute_units_consumed: Some(40_000),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }))
}

fn entry(slot: Slot, index: u64) -> SubscribeUpdate {
    update(UpdateOneof::Entry(SubscribeUpdateEntry {
        slot,
        index,
        hash: vec![index as u8; 32],
        executed_transaction_count: 1,
        starting_transaction_index: index,
        ..Default::default()
    }))
}

fn complete_slot(slot_number: Slot) -> Vec<SubscribeUpdate> {
    let mut messages = vec![slot(slot_number, SlotStatus::SlotCreatedBank)];
    for index in 0..3 {
        messages.extend([
            account(slot_number, index, false),
            transaction(slot_number, index),
            entry(slot_number, index),
        ]);
    }
    messages.extend([
        account(slot_number, 3, true),
        account(slot_number, 4, true),
        update(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot: slot_number,
            blockhash: "11111111111111111111111111111111".to_owned(),
            block_height: Some(BlockHeight {
                block_height: slot_number,
            }),
            executed_transaction_count: 3,
            entries_count: 3,
            ..Default::default()
        })),
        slot(slot_number, SlotStatus::SlotProcessed),
        slot(slot_number, SlotStatus::SlotConfirmed),
        slot(slot_number, SlotStatus::SlotFinalized),
    ]);
    messages
}

fn banked(mut message: SubscribeUpdate) -> SubscribeUpdate {
    match message.update_oneof.as_mut().unwrap() {
        UpdateOneof::Slot(message) => message.bank_id = Some(message.slot * 12_345),
        UpdateOneof::Account(message) => message.bank_id = Some(message.slot * 12_345),
        UpdateOneof::Transaction(message) => message.bank_id = message.slot * 12_345,
        UpdateOneof::Entry(message) => message.bank_id = message.slot * 12_345,
        UpdateOneof::BlockMeta(message) => message.bank_id = message.slot * 12_345,
        _ => unreachable!(),
    }
    message
}

fn encoded(message: &ParsedMessage) -> SubscribeUpdate {
    SubscribeUpdate::decode(
        FilteredUpdate {
            filters: Default::default(),
            filtered_update: richat_filter::message::MessageRef::from(message).into(),
        }
        .encode_to_vec()
        .as_slice(),
    )
    .unwrap()
}

fn assert_complete(messages: &Messages, head: u64, expected: &[SubscribeUpdate]) {
    let receiver = messages.to_receiver();
    for commitment in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        let mut received = vec![];
        let mut cursor = head;
        while let Some(message) = receiver.try_recv(commitment, cursor).unwrap() {
            received.push(encoded(&message));
            cursor += 1;
        }
        let expected: Vec<_> = expected
            .iter()
            .filter(|message| {
                if commitment == CommitmentLevel::Processed {
                    return true;
                }
                let Some(UpdateOneof::Account(account)) = &message.update_oneof else {
                    return true;
                };
                let info = account.account.as_ref().unwrap();
                !expected.iter().any(|other| {
                    matches!(&other.update_oneof, Some(UpdateOneof::Account(other))
                    if other.slot == account.slot
                        && other.account.as_ref().unwrap().pubkey == info.pubkey
                        && other.account.as_ref().unwrap().write_version > info.write_version)
                })
            })
            .collect();
        for expected_message in &expected {
            let expected_oneof = expected_message.update_oneof.as_ref().unwrap();
            if matches!(expected_oneof, UpdateOneof::Slot(_)) {
                continue;
            }
            assert_eq!(
                received
                    .iter()
                    .filter(|message| message.update_oneof.as_ref() == Some(expected_oneof))
                    .count(),
                1,
                "missing, duplicated or modified message at {commitment:?}: {expected_oneof:?}"
            );
        }
        let counts = received.iter().fold([0; 5], |mut counts, message| {
            match message.update_oneof.as_ref().unwrap() {
                UpdateOneof::Slot(message) => {
                    assert_eq!(message.bank_id, Some(message.slot * 12_345));
                }
                UpdateOneof::Transaction(_) => counts[0] += 1,
                UpdateOneof::Account(_) => counts[1] += 1,
                UpdateOneof::Entry(_) => counts[2] += 1,
                UpdateOneof::BlockMeta(_) => counts[3] += 1,
                UpdateOneof::Block(block) => {
                    counts[4] += 1;
                    assert_eq!(block.bank_id, block.slot * 12_345);
                    assert_eq!(block.transactions.len(), 3);
                    assert_eq!(block.entries.len(), 3);
                    assert_eq!(block.accounts.len(), 3);
                    assert!(
                        block
                            .accounts
                            .iter()
                            .any(|account| account.pubkey == vec![3; 32]
                                && account.lamports == block.slot + 50)
                    );
                    assert!(
                        block.transactions[2]
                            .transaction
                            .as_ref()
                            .unwrap()
                            .message
                            .as_ref()
                            .unwrap()
                            .config
                            .is_some()
                    );
                }
                _ => {}
            }
            counts
        });
        let expected_accounts = if commitment == CommitmentLevel::Processed {
            10
        } else {
            6
        };
        assert_eq!(
            counts,
            [6, expected_accounts, 6, 2, 2],
            "incomplete {commitment:?} stream"
        );
        let received_data: Vec<_> = received
            .iter()
            .filter_map(|message| match &message.update_oneof {
                Some(UpdateOneof::Slot(_) | UpdateOneof::Block(_)) => None,
                message => message.as_ref(),
            })
            .collect();
        let expected_data: Vec<_> = expected
            .iter()
            .filter_map(|message| match &message.update_oneof {
                Some(UpdateOneof::Slot(_)) => None,
                message => message.as_ref(),
            })
            .collect();
        assert_eq!(
            received_data, expected_data,
            "modified order or payload at {commitment:?}"
        );
    }
}

async fn run_reconnect_case(
    parser: MessageParserEncoding,
    interruptions: usize,
    decreasing_versions: bool,
) {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut complete: Vec<_> = complete_slot(100).into_iter().map(banked).collect();
    if decreasing_versions {
        let Some(UpdateOneof::Account(account)) = &mut complete[1].update_oneof else {
            unreachable!()
        };
        account.account.as_mut().unwrap().write_version += 2;
    }
    let mut batches = VecDeque::new();
    let mut initial = vec![banked(slot(99, SlotStatus::SlotFinalized))];
    initial.extend_from_slice(&complete[..4]);
    batches.push_back(initial);
    if interruptions > 1 {
        batches.push_back(complete[..8].to_vec());
    }
    let expected = [
        complete,
        complete_slot(101).into_iter().map(banked).collect(),
    ]
    .concat();
    batches.push_back(expected.clone());
    let shutdown = CancellationToken::new();
    let upstream = ReplayUpstream {
        batches: Arc::new(Mutex::new(batches)),
        requests: Arc::new(Mutex::new(vec![])),
        reject_replay: false,
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
        upstream.clone(),
        || {},
        || {},
        VERSION,
        shutdown.clone(),
    )
    .await
    .unwrap();
    let server_task = tokio::spawn(server);
    let config = config(format!("http://{address}"), parser);
    let (mut messages, _) = Messages::new(
        parser,
        config.config.clone(),
        true,
        true,
        true,
        shutdown.clone(),
    )
    .unwrap();
    let head = messages.get_current_tail(CommitmentLevel::Processed) + 1;
    let (mut sender, replay) = messages.to_sender(1).unwrap();
    let mode = config.dedup_mode();
    let mut subscriptions = Subscriptions::new(config.sources, replay).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let (source, message) = subscriptions.next().await.unwrap().unwrap();
            let finished = matches!(&message, Message::Slot(message) if message.slot() == 101 && message.status() == SlotStatus::SlotFinalized);
            sender.push(mode, source, message).unwrap();
            if finished { break; }
        }
    }).await.unwrap();
    shutdown.cancel();
    timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut expected_requests = vec![None];
    expected_requests.extend(std::iter::repeat_n(Some(100), interruptions));
    assert_eq!(*upstream.requests.lock().unwrap(), expected_requests);
    assert_complete(&messages, head, &expected);
    assert_grpc_replay(&messages, head).await;
}

async fn assert_grpc_replay(messages: &Messages, head: u64) {
    let shutdown = CancellationToken::new();
    let address = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut config = ConfigAppsGrpc::default();
    config.server.endpoint = address;
    config.unary.enabled = false;
    config.workers.messages_cached_max = 16;
    let server = FanoutServer::spawn(config, messages.clone(), shutdown.clone()).unwrap();
    let server_task = tokio::spawn(server);
    let mut client = ConfigGrpcClient {
        endpoint: format!("http://{address}"),
        ..Default::default()
    }
    .connect()
    .await
    .unwrap();
    for commitment in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        let receiver = messages.to_receiver();
        let mut wanted = vec![];
        let mut cursor = head;
        while let Some(message) = receiver.try_recv(commitment, cursor).unwrap() {
            if !matches!(message, ParsedMessage::Slot(_) | ParsedMessage::Block(_)) {
                wanted.push(encoded(&message).update_oneof.unwrap());
            }
            cursor += 1;
        }
        let mut request = Subscription::create_dragons_mouth_filter(false, false, false, Some(100));
        request.commitment = Some(commitment as i32);
        let mut stream = client.subscribe_dragons_mouth_once(request).await.unwrap();
        if commitment != CommitmentLevel::Processed {
            let error = timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            let richat_client::error::ReceiveError::Status(status) = error else {
                panic!("unexpected replay error: {error:?}");
            };
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            assert!(
                status
                    .message()
                    .contains("available only for `processed` commitment")
            );
            continue;
        }
        let received = timeout(Duration::from_secs(5), async {
            let mut received = vec![];
            while received.len() < wanted.len() {
                let message = stream.next().await.unwrap().unwrap();
                let update = SubscribeUpdate::decode(message.as_slice())
                    .unwrap()
                    .update_oneof
                    .unwrap();
                if let UpdateOneof::Slot(message) = &update {
                    assert_eq!(message.bank_id, Some(message.slot * 12_345));
                }
                if !matches!(
                    update,
                    UpdateOneof::Slot(_) | UpdateOneof::Ping(_) | UpdateOneof::Pong(_)
                ) {
                    received.push(update);
                }
            }
            received
        })
        .await
        .unwrap();
        assert_eq!(received, wanted, "incomplete gRPC replay at {commitment:?}");
    }
    drop(client);
    shutdown.cancel();
    timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn single_reconnecting_source_enables_dedup() {
    assert_eq!(
        config(
            "http://127.0.0.1:1".to_owned(),
            MessageParserEncoding::Prost
        )
        .dedup_mode(),
        DedupMode::SingleSource
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_source_grpc_reconnect_preserves_every_message_and_block() {
    run_reconnect_case(MessageParserEncoding::Prost, 1, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_single_source_grpc_reconnect_preserves_partial_accounts_and_v1() {
    run_reconnect_case(MessageParserEncoding::Prost, 2, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limited_parser_repeated_reconnect_preserves_partial_accounts_and_v1() {
    run_reconnect_case(MessageParserEncoding::Limited, 2, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_reconnect_preserves_latest_canonical_account_with_decreasing_versions() {
    run_reconnect_case(MessageParserEncoding::Prost, 2, true).await;
}

#[test]
fn dedup_mode_preserves_non_reconnecting_and_multi_source_behavior() {
    let mut config = config(
        "http://127.0.0.1:1".to_owned(),
        MessageParserEncoding::Prost,
    );
    let ConfigChannelSource::Grpc { general, .. } = &mut config.sources[0] else {
        unreachable!()
    };
    general.reconnect = None;
    assert_eq!(config.dedup_mode(), DedupMode::Disabled);
    config.sources_sighup_reload = true;
    assert_eq!(config.dedup_mode(), DedupMode::MultiSource);
    config.sources_sighup_reload = false;
    config.sources.push(config.sources[0].clone());
    assert_eq!(config.dedup_mode(), DedupMode::MultiSource);
}

#[test]
fn single_quic_source_with_reconnect_enables_single_source_dedup() {
    let mut config = config(
        "http://127.0.0.1:1".to_owned(),
        MessageParserEncoding::Prost,
    );
    let ConfigChannelSource::Grpc { general, .. } = config.sources.pop().unwrap() else {
        unreachable!()
    };
    config.sources.push(ConfigChannelSource::Quic {
        general,
        config: ConfigQuicClient::default(),
    });
    assert_eq!(config.dedup_mode(), DedupMode::SingleSource);
}

#[test]
fn multi_source_dedup_still_normalizes_cross_source_account_versions() {
    let config = config(
        "http://127.0.0.1:1".to_owned(),
        MessageParserEncoding::Prost,
    );
    let (mut messages, _) = Messages::new(
        MessageParserEncoding::Prost,
        config.config,
        true,
        true,
        true,
        CancellationToken::new(),
    )
    .unwrap();
    let head = messages.get_next_index(CommitmentLevel::Processed);
    let (mut sender, _) = messages.to_sender(2).unwrap();
    let mut first = account(100, 0, false);
    let Some(UpdateOneof::Account(first_account)) = &mut first.update_oneof else {
        unreachable!()
    };
    first_account.bank_id = Some(0);
    let mut second = first.clone();
    let Some(UpdateOneof::Account(account)) = &mut second.update_oneof else {
        unreachable!()
    };
    account.account.as_mut().unwrap().write_version += 10_000;
    for (source, update) in [
        ("one", first),
        ("two", second),
        ("one", transaction(100, 0)),
        ("two", transaction(100, 0)),
    ] {
        sender
            .push(
                DedupMode::MultiSource,
                source,
                Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
                    .unwrap(),
            )
            .unwrap();
    }
    let receiver = messages.to_receiver();
    let Some(ParsedMessage::Account(account)) =
        receiver.try_recv(CommitmentLevel::Processed, head).unwrap()
    else {
        panic!("missing account");
    };
    assert_eq!(account.write_version(), 1_000);
    assert!(matches!(
        receiver
            .try_recv(CommitmentLevel::Processed, head + 1)
            .unwrap(),
        Some(ParsedMessage::Transaction(_))
    ));
    assert!(
        receiver
            .try_recv(CommitmentLevel::Processed, head + 2)
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_replay_is_an_error_without_silent_live_fallback() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let shutdown = CancellationToken::new();
    let upstream = ReplayUpstream {
        batches: Arc::new(Mutex::new(VecDeque::from([vec![slot(
            99,
            SlotStatus::SlotFinalized,
        )]]))),
        requests: Arc::new(Mutex::new(vec![])),
        reject_replay: true,
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
        upstream.clone(),
        || {},
        || {},
        VERSION,
        shutdown.clone(),
    )
    .await
    .unwrap();
    let server_task = tokio::spawn(server);
    let config = config(format!("http://{address}"), MessageParserEncoding::Prost);
    let (mut messages, _) = Messages::new(
        MessageParserEncoding::Prost,
        config.config.clone(),
        true,
        true,
        true,
        shutdown.clone(),
    )
    .unwrap();
    let (mut sender, replay) = messages.to_sender(1).unwrap();
    let mut subscriptions = Subscriptions::new(config.sources, replay).await.unwrap();
    timeout(Duration::from_secs(5), async {
        let (source, message) = subscriptions.next().await.unwrap().unwrap();
        sender
            .push(DedupMode::SingleSource, source, message)
            .unwrap();
        assert!(matches!(
            subscriptions.next().await.unwrap(),
            Err(ReceiveError::ReplayFailed)
        ));
        assert!(subscriptions.next().await.is_none());
    })
    .await
    .unwrap();
    shutdown.cancel();
    timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*upstream.requests.lock().unwrap(), vec![None, Some(100)]);
}
