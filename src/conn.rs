// Per-connection protocol loop: parse frames, enforce the abuse limits (rate, topic
// count, frame size), and bridge between the WebSocket and the hub. One spawned task
// per connection plus one writer task; the hub never blocks on a socket.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::info;

use crate::config::RelayConfig;
use crate::hub::Hub;
use crate::protocol::{ClientFrame, ServerFrame, topic_is_valid};

/// Sustained-rate token bucket. Refills continuously; one token per inbound frame.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    rate_per_sec: f64,
    burst: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            last_refill: Instant::now(),
            rate_per_sec,
            burst,
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Outbound queue depth per connection. Bounded so a stalled reader disconnects
/// instead of buffering unboundedly inside the relay.
const OUTBOUND_QUEUE: usize = 256;

pub async fn run_connection(
    socket: WebSocket,
    conn_id: u64,
    hub: Arc<Hub>,
    config: Arc<RelayConfig>,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(OUTBOUND_QUEUE);

    // Writer task: the single owner of the sink. Everything (acks, errors, delivers
    // from other connections via the hub) funnels through the queue. It also drives
    // the server-side keepalive ping: clients auto-answer with pongs, keeping BOTH
    // directions of proxied tunnels (Cloudflare/nginx) active - browsers cannot send
    // pings themselves, so the server must originate them.
    let ping_secs = config.keepalive_ping_secs;
    let writer = tokio::spawn(async move {
        // A zero interval would busy-loop; when pings are disabled the timer still
        // exists (select! needs a future) but its arm is gated off.
        let mut ping = tokio::time::interval(Duration::from_secs(ping_secs.max(1)));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the first tick fires immediately; skip it
        loop {
            tokio::select! {
                frame = out_rx.recv() => match frame {
                    Some(frame) => {
                        if ws_tx.send(Message::Text(frame.into())).await.is_err() {
                            return;
                        }
                    }
                    None => break,
                },
                _ = ping.tick(), if ping_secs > 0 => {
                    if ws_tx.send(Message::Ping(Vec::new().into())).await.is_err() {
                        return;
                    }
                }
            }
        }
        // Channel closed cleanly: tell the peer before dropping the socket.
        let _ = ws_tx.send(Message::Close(None)).await;
    });

    let mut bucket = TokenBucket::new(config.rate_limit_frames_per_sec, config.rate_burst);
    let idle = Duration::from_secs(config.idle_timeout_secs);
    let mut subscribed: Vec<String> = Vec::new();
    // Why the loop ended, for the "conn closed" log line. Defaults to a clean peer close.
    let mut close_reason = "peer_closed";

    loop {
        // Idle gate counts ANY inbound traffic (pings included: axum answers them and
        // still yields the message here), so a quiet-but-alive subscriber survives.
        let message = match tokio::time::timeout(idle, ws_rx.next()).await {
            Err(_) => {
                close_reason = "idle_timeout";
                break;
            }
            Ok(None) => break, // peer closed the TCP/WS stream
            Ok(Some(Err(_))) => {
                close_reason = "ws_error";
                break;
            }
            Ok(Some(Ok(message))) => message,
        };

        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => {
                close_reason = "close_frame";
                break;
            }
            // Pings/pongs are connection upkeep, not protocol frames; binary is not
            // part of the relay protocol and is ignored rather than fatal.
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => continue,
        };

        if !bucket.allow() {
            // Rate abuse ends the connection: this relay only serves our own SDK and
            // wallet, which never legitimately exceed the configured rate.
            send(
                &out_tx,
                ServerFrame::Error {
                    code: "rate_limited",
                    message: "Inbound frame rate exceeded",
                    id: None,
                },
            )
            .await;
            break;
        }

        // The ws layer caps messages slightly ABOVE this (see server setup), so this
        // check is reachable and produces a clean protocol error instead of an abort.
        if text.len() > config.frame_cap_bytes {
            send(
                &out_tx,
                ServerFrame::Error {
                    code: "frame_too_large",
                    message: "Frame exceeds the relay frame cap",
                    id: None,
                },
            )
            .await;
            break;
        }

        let frame: ClientFrame = match serde_json::from_str(text.as_str()) {
            Ok(frame) => frame,
            Err(_) => {
                send(
                    &out_tx,
                    ServerFrame::Error {
                        code: "bad_frame",
                        message: "Frame is not a valid relay message",
                        id: None,
                    },
                )
                .await;
                break;
            }
        };

        match frame {
            ClientFrame::Subscribe { topic } => {
                if !topic_is_valid(&topic) {
                    send(
                        &out_tx,
                        ServerFrame::Error {
                            code: "bad_topic",
                            message: "Invalid topic",
                            id: None,
                        },
                    )
                    .await;
                    break;
                }
                if subscribed.iter().any(|t| t == &topic) {
                    continue; // idempotent re-subscribe
                }
                if subscribed.len() >= config.max_topics_per_conn {
                    send(
                        &out_tx,
                        ServerFrame::Error {
                            code: "topic_limit",
                            message: "Too many topics on one connection",
                            id: None,
                        },
                    )
                    .await;
                    continue;
                }
                hub.subscribe(&topic, conn_id, out_tx.clone());
                info!(conn_id, topic = %topic, "subscribe");
                subscribed.push(topic);
            }

            ClientFrame::Unsubscribe { topic } => {
                hub.unsubscribe(&topic, conn_id);
                info!(conn_id, topic = %topic, "unsubscribe");
                subscribed.retain(|t| t != &topic);
            }

            ClientFrame::Publish { topic, id, payload } => {
                if !topic_is_valid(&topic) {
                    send(
                        &out_tx,
                        ServerFrame::Error {
                            code: "bad_topic",
                            message: "Invalid topic",
                            id: id.as_deref(),
                        },
                    )
                    .await;
                    break;
                }
                let deliver = ServerFrame::Deliver {
                    topic: &topic,
                    id: id.as_deref(),
                    payload: &payload,
                }
                .to_json();
                let bytes = deliver.len();
                let delivered = hub.publish(&topic, conn_id, deliver);
                // Routing visibility (permanent): delivered = live subscribers that got it;
                // 0 means it went to the TTL mailbox (no peer subscribed right now). This is
                // the line that shows whether a dApp request actually reached the wallet.
                info!(conn_id, topic = %topic, bytes, delivered, "publish");
                // Ack = accepted (delivered now or mailboxed); the publisher does not
                // learn HOW many peers got it, that is session-layer business.
                send(
                    &out_tx,
                    ServerFrame::Ack {
                        topic: &topic,
                        id: id.as_deref(),
                    },
                )
                .await;
            }
        }
    }

    hub.drop_connection(conn_id, &subscribed);
    drop(out_tx); // closes the queue; the writer drains and sends Close
    let _ = writer.await;
    info!(
        conn_id,
        reason = close_reason,
        topics = subscribed.len(),
        "conn closed"
    );
}

async fn send(out_tx: &mpsc::Sender<String>, frame: ServerFrame<'_>) {
    let _ = out_tx.send(frame.to_json()).await;
}
