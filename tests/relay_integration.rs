// Integration tests over a REAL WebSocket: every test boots the relay on an ephemeral
// localhost port and talks to it with tokio-tungstenite, exactly as the SDK transport
// will. Frames are asserted as JSON values, pinning the section-18 wire format.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use phantasma_link_relay::config::RelayConfig;

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn start_relay(config: RelayConfig) -> std::net::SocketAddr {
    let (addr, _handle) = phantasma_link_relay::start(config)
        .await
        .expect("relay starts");
    addr
}

fn test_config() -> RelayConfig {
    RelayConfig {
        // Port 0 = the OS picks a free port; start() reports the real one.
        bind: "localhost:0".to_string(),
        ..RelayConfig::default()
    }
}

async fn connect(addr: std::net::SocketAddr) -> Client {
    let (client, _) = connect_async(format!("ws://localhost:{}/relay", addr.port()))
        .await
        .expect("ws connect");
    client
}

async fn send_json(client: &mut Client, value: Value) {
    client
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("send");
}

/// Receive the next TEXT frame as JSON, skipping connection upkeep frames.
async fn recv_json(client: &mut Client) -> Value {
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), client.next())
            .await
            .expect("frame within 5s")
            .expect("stream open")
            .expect("frame ok");
        match message {
            Message::Text(text) => return serde_json::from_str(text.as_str()).expect("json"),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

// The core route: a publish reaches the OTHER subscriber of the topic (not the
// publisher itself), the publisher gets an ack, and ids are echoed end to end.
#[tokio::test]
async fn publish_reaches_other_subscribers_with_ack() {
    let addr = start_relay(test_config()).await;
    let mut wallet = connect(addr).await;
    let mut dapp = connect(addr).await;

    send_json(&mut wallet, json!({"op": "subscribe", "topic": "t-1"})).await;
    send_json(&mut dapp, json!({"op": "subscribe", "topic": "t-1"})).await;
    // Subscribe is silent; prove ordering by completing a full round-trip below.

    send_json(
        &mut dapp,
        json!({"op": "publish", "topic": "t-1", "id": "m1", "payload": {"nonce": "AA", "ct": "BB"}}),
    )
    .await;

    let ack = recv_json(&mut dapp).await;
    assert_eq!(ack, json!({"op": "ack", "topic": "t-1", "id": "m1"}));

    let delivered = recv_json(&mut wallet).await;
    assert_eq!(
        delivered,
        json!({"op": "deliver", "topic": "t-1", "id": "m1", "payload": {"nonce": "AA", "ct": "BB"}})
    );

    // The publisher must NOT receive its own frame: publish from the wallet next and
    // assert the dapp sees it while the wallet sees only its ack (then silence).
    send_json(
        &mut wallet,
        json!({"op": "publish", "topic": "t-1", "payload": "reply"}),
    )
    .await;
    assert_eq!(
        recv_json(&mut wallet).await,
        json!({"op": "ack", "topic": "t-1"})
    );
    assert_eq!(
        recv_json(&mut dapp).await,
        json!({"op": "deliver", "topic": "t-1", "payload": "reply"})
    );
}

// Mailbox (spec: deeplink-woken wallet): a publish with no subscriber is held and
// flushed the moment a subscriber appears.
#[tokio::test]
async fn mailbox_holds_for_late_subscriber() {
    let addr = start_relay(test_config()).await;
    let mut dapp = connect(addr).await;

    send_json(
        &mut dapp,
        json!({"op": "publish", "topic": "t-wake", "id": "w1", "payload": "sealed-request"}),
    )
    .await;
    assert_eq!(
        recv_json(&mut dapp).await,
        json!({"op": "ack", "topic": "t-wake", "id": "w1"})
    );

    let mut wallet = connect(addr).await;
    send_json(&mut wallet, json!({"op": "subscribe", "topic": "t-wake"})).await;
    assert_eq!(
        recv_json(&mut wallet).await,
        json!({"op": "deliver", "topic": "t-wake", "id": "w1", "payload": "sealed-request"})
    );
}

// Expired mailbox entries must NOT be delivered (and the sweep keeps memory bounded).
#[tokio::test]
async fn mailbox_entries_expire_after_ttl() {
    let mut config = test_config();
    config.mailbox_ttl_secs = 1;
    let addr = start_relay(config).await;

    let mut dapp = connect(addr).await;
    send_json(
        &mut dapp,
        json!({"op": "publish", "topic": "t-ttl", "payload": "stale"}),
    )
    .await;
    assert_eq!(
        recv_json(&mut dapp).await,
        json!({"op": "ack", "topic": "t-ttl"})
    );

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    let mut wallet = connect(addr).await;
    send_json(&mut wallet, json!({"op": "subscribe", "topic": "t-ttl"})).await;
    // Prove silence by racing a fresh publish: the only deliver must be the new one.
    send_json(
        &mut dapp,
        json!({"op": "publish", "topic": "t-ttl", "payload": "fresh"}),
    )
    .await;
    assert_eq!(
        recv_json(&mut wallet).await,
        json!({"op": "deliver", "topic": "t-ttl", "payload": "fresh"})
    );
}

// Unsubscribe stops delivery for that topic while other topics keep flowing.
#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let addr = start_relay(test_config()).await;
    let mut a = connect(addr).await;
    let mut b = connect(addr).await;

    send_json(&mut a, json!({"op": "subscribe", "topic": "t-x"})).await;
    send_json(&mut a, json!({"op": "subscribe", "topic": "t-y"})).await;
    send_json(&mut a, json!({"op": "unsubscribe", "topic": "t-x"})).await;
    // Ordering is only guaranteed WITHIN one connection, so b must not publish until
    // a's unsubscribe is processed. The ack to a's own publish is that barrier: when
    // it arrives, every earlier frame from a (incl. the unsubscribe) has been handled.
    send_json(
        &mut a,
        json!({"op": "publish", "topic": "t-sync", "payload": 0}),
    )
    .await;
    assert_eq!(
        recv_json(&mut a).await,
        json!({"op": "ack", "topic": "t-sync"})
    );

    send_json(
        &mut b,
        json!({"op": "publish", "topic": "t-x", "payload": 1}),
    )
    .await;
    send_json(
        &mut b,
        json!({"op": "publish", "topic": "t-y", "payload": 2}),
    )
    .await;

    // Only the t-y frame may arrive at `a`.
    assert_eq!(
        recv_json(&mut a).await,
        json!({"op": "deliver", "topic": "t-y", "payload": 2})
    );
}

// Limit: a connection cannot hold more topics than configured; the violation is a
// protocol error frame (connection stays usable for the existing topics).
#[tokio::test]
async fn topic_limit_is_enforced() {
    let mut config = test_config();
    config.max_topics_per_conn = 2;
    let addr = start_relay(config).await;

    let mut client = connect(addr).await;
    send_json(&mut client, json!({"op": "subscribe", "topic": "t-1"})).await;
    send_json(&mut client, json!({"op": "subscribe", "topic": "t-2"})).await;
    send_json(&mut client, json!({"op": "subscribe", "topic": "t-3"})).await;

    let error = recv_json(&mut client).await;
    assert_eq!(error["op"], "error");
    assert_eq!(error["code"], "topic_limit");
}

// Limit: frames above the configured cap produce a clean protocol error and close.
#[tokio::test]
async fn oversized_frames_are_rejected() {
    let mut config = test_config();
    config.frame_cap_bytes = 4096;
    let addr = start_relay(config).await;

    let mut client = connect(addr).await;
    let big = "x".repeat(5000);
    send_json(
        &mut client,
        json!({"op": "publish", "topic": "t", "payload": big}),
    )
    .await;

    let error = recv_json(&mut client).await;
    assert_eq!(error["op"], "error");
    assert_eq!(error["code"], "frame_too_large");
}

// Malformed and unknown frames are protocol violations: one error frame, then close.
#[tokio::test]
async fn malformed_frames_get_an_error_and_close() {
    let addr = start_relay(test_config()).await;
    let mut client = connect(addr).await;

    client
        .send(Message::Text("not json at all".into()))
        .await
        .expect("send");

    let error = recv_json(&mut client).await;
    assert_eq!(error["op"], "error");
    assert_eq!(error["code"], "bad_frame");

    // The relay closes after a protocol violation; the stream must end.
    loop {
        match client.next().await {
            None => break,
            Some(Ok(Message::Close(_))) => break,
            Some(Ok(_)) => continue,
            Some(Err(_)) => break,
        }
    }
}

// Keepalive: the SERVER originates WebSocket pings (browsers cannot), so proxied idle
// tunnels stay active in both directions; the client library auto-answers with pongs.
#[tokio::test]
async fn server_pings_idle_connections() {
    let mut config = test_config();
    config.keepalive_ping_secs = 1;
    let addr = start_relay(config).await;

    let mut client = connect(addr).await;
    send_json(&mut client, json!({"op": "subscribe", "topic": "t-idle"})).await;

    // An otherwise idle subscriber must see a Ping within ~2 intervals.
    let deadline = std::time::Duration::from_secs(3);
    let got_ping = tokio::time::timeout(deadline, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Ping(_))) => return true,
                Some(Ok(_)) => continue,
                _ => return false,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        got_ping,
        "expected a server keepalive ping on an idle connection"
    );
}

// Limit: connections beyond the per-IP cap are refused at the HTTP layer.
#[tokio::test]
async fn per_ip_connection_cap_is_enforced() {
    let mut config = test_config();
    config.max_conns_per_ip = 2;
    let addr = start_relay(config).await;

    let _c1 = connect(addr).await;
    let _c2 = connect(addr).await;
    let refused = connect_async(format!("ws://localhost:{}/relay", addr.port())).await;
    assert!(
        refused.is_err(),
        "third connection from one IP must be refused"
    );
}
