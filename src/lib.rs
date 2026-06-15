// Phantasma Link v5 relay (spec section 18): a dumb, E2E-blind pub/sub over WebSocket.
// Clients publish OPAQUE ciphertext frames to a topic; every other subscriber of that
// topic receives them; a topic with no subscribers holds frames in a TTL mailbox. The
// relay holds no keys, parses no payloads, and knows nothing about the chain - all
// trust lives in the NaCl channel between dApp and wallet.
//
// TLS is OUT of scope on purpose: the relay binds localhost behind the reverse proxy
// on link.phantasma.info (see the hosting kit), which terminates WSS.

pub mod config;
pub mod conn;
pub mod hub;
pub mod protocol;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::config::RelayConfig;
use crate::hub::Hub;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RelayConfig>,
    pub hub: Arc<Hub>,
    next_conn_id: Arc<AtomicU64>,
    conns_per_ip: Arc<Mutex<HashMap<IpAddr, u32>>>,
}

impl AppState {
    pub fn new(config: RelayConfig) -> Self {
        let hub = Hub::new(
            Duration::from_secs(config.mailbox_ttl_secs),
            config.mailbox_max_frames,
        );
        Self {
            config: Arc::new(config),
            hub: Arc::new(hub),
            next_conn_id: Arc::new(AtomicU64::new(1)),
            conns_per_ip: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/relay", get(ws_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

async fn ws_handler(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    // Per-IP connection cap, checked BEFORE the upgrade so abuse is refused cheaply.
    // Behind the reverse proxy every peer is the proxy's address, so the cap then acts
    // as a global ceiling; per-client fairness on the public host comes from the
    // proxy's own limits. Direct (localhost/test) use gets true per-IP behavior.
    let ip = peer.ip();
    {
        let mut conns = state.conns_per_ip.lock().expect("ip map lock");
        let count = conns.entry(ip).or_insert(0);
        if *count >= state.config.max_conns_per_ip {
            debug!(%ip, "connection cap reached");
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        *count += 1;
    }

    let conn_id = state.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let hub = state.hub.clone();
    let config = state.config.clone();
    let conns_per_ip = state.conns_per_ip.clone();

    upgrade
        // The ws cap sits one KiB ABOVE the protocol cap so an oversized-but-parseable
        // frame gets a clean protocol error from conn.rs; anything beyond is cut at the
        // transport. Both bound relay memory per frame.
        .max_message_size(config.frame_cap_bytes + 1024)
        .max_frame_size(config.frame_cap_bytes + 1024)
        .on_upgrade(move |socket| async move {
            info!(conn_id, %ip, "conn open");
            conn::run_connection(socket, conn_id, hub, config).await;
            // Connection accounting must outlive the protocol loop, whatever way it ends.
            let mut conns = conns_per_ip.lock().expect("ip map lock");
            if let Some(count) = conns.get_mut(&ip) {
                *count -= 1;
                if *count == 0 {
                    conns.remove(&ip);
                }
            }
        })
        .into_response()
}

/// Bind the configured address, start serving, and start the periodic mailbox sweep.
/// Returns the actually bound address (port 0 resolves here, which tests rely on).
pub async fn start(config: RelayConfig) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(&config.bind).await?;
    let addr = listener.local_addr()?;
    let state = AppState::new(config);

    // Lazy pruning on access would let fully-abandoned topics linger forever; this
    // sweep guarantees expired mailboxes are reclaimed even with zero traffic.
    let sweep_hub = state.hub.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            sweep_hub.prune();
        }
    });

    let router = app(state);
    info!(%addr, "relay listening");
    let handle = tokio::spawn(async move {
        // ConnectInfo gives the per-connection peer address used by the IP cap.
        if let Err(err) = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            tracing::error!(%err, "relay server stopped");
        }
    });

    Ok((addr, handle))
}
