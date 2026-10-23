use {
    super::*,
    fastwebsockets::{Role, WebSocket},
    serde_json::{Value, json},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::{Duration, timeout},
    },
};

async fn connected_client() -> (
    WebSocket<TcpStream>,
    kanal::AsyncReceiver<ClientRequest>,
    broadcast::Sender<RpcNotification>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (clients_tx, clients_rx) = kanal::bounded_async(8);
    let (notifications, _) = broadcast::channel(8);
    let server_notifications = notifications.clone();
    let (done_tx, done_rx) = oneshot::channel();
    let done_tx = Arc::new(std::sync::Mutex::new(Some(done_tx)));
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(move |request| {
            let (response, ws_fut) = upgrade(request).unwrap();
            let clients_tx = clients_tx.clone();
            let notifications = server_notifications.subscribe();
            let done_tx = done_tx.lock().unwrap().take().unwrap();
            tokio::spawn(async move {
                let result = PubSubServer::handle_client(
                    42,
                    Arc::from("lifecycle-test"),
                    ws_fut,
                    4096,
                    false,
                    false,
                    clients_tx,
                    notifications,
                    CancellationToken::new(),
                )
                .await;
                let _ = done_tx.send(result);
            });
            async { Ok::<_, std::convert::Infallible>(response) }
        });
        ServerBuilder::new(TokioExecutor::new())
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .await
            .unwrap();
        let _ = done_rx.await.unwrap();
    });
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!(
        "GET / HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    ).as_bytes()).await.unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        headers.push(stream.read_u8().await.unwrap());
    }
    assert!(headers.starts_with(b"HTTP/1.1 101"));
    (
        WebSocket::after_handshake(stream, Role::Client),
        clients_rx,
        notifications,
        server,
    )
}

#[tokio::test]
async fn websocket_subscription_stays_registered_until_actual_disconnect() {
    let (mut websocket, requests, _notifications, server) = connected_client().await;
    assert!(requests.try_recv().unwrap().is_none());
    websocket
        .write_frame(Frame::text(Payload::Owned(
            br#"{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}"#.to_vec(),
        )))
        .await
        .unwrap();
    let request = timeout(Duration::from_secs(1), requests.recv())
        .await
        .unwrap()
        .unwrap();
    let ClientRequest::Subscribe { client_id, tx, .. } = request else {
        panic!("expected subscribe");
    };
    assert_eq!(client_id, 42);
    tx.send((7, SubscribeMethod::Slot)).unwrap();
    let response = websocket.read_frame().await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.payload).unwrap(),
        json!({"jsonrpc":"2.0","id":1,"result":7})
    );
    websocket
        .write_frame(Frame::new(
            true,
            OpCode::Ping,
            None,
            Payload::Owned(vec![1, 2, 3]),
        ))
        .await
        .unwrap();
    let pong = websocket.read_frame().await.unwrap();
    assert_eq!(pong.opcode, OpCode::Pong);
    assert!(requests.try_recv().unwrap().is_none());
    drop(websocket);
    assert!(matches!(
        timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap(),
        ClientRequest::Remove { client_id: 42 }
    ));
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn websocket_writer_exit_cancels_reader_and_unregisters_client() {
    let (mut websocket, requests, notifications, server) = connected_client().await;
    drop(notifications);
    assert!(matches!(
        timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap(),
        ClientRequest::Remove { client_id: 42 }
    ));
    assert_eq!(websocket.read_frame().await.unwrap().opcode, OpCode::Close);
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
