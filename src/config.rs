// Relay configuration (spec section 18 limits). Every knob has a safe default so the
// binary runs with no config file at all; a TOML file overrides individual fields.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelayConfig {
    /// Listen address. The relay terminates plain WS only: TLS lives in the reverse
    /// proxy (Caddy on link.phantasma.info), so the default binds localhost and the
    /// relay must never be exposed directly.
    pub bind: String,
    /// Maximum size of one relay frame in bytes (spec default 1 MiB). Messages larger
    /// than this must be chunked by the CLIENTS ({msgId, seq, total, chunk} inside the
    /// opaque payload); the relay never reassembles, it only bounds per-frame memory.
    pub frame_cap_bytes: usize,
    /// How long an undelivered publish is held for a topic with no subscribers.
    pub mailbox_ttl_secs: u64,
    /// Mailbox depth per topic; when full the OLDEST frame is dropped (pairing and
    /// request flows care about the newest state, and the bound keeps memory finite).
    pub mailbox_max_frames: usize,
    /// Maximum concurrently subscribed topics per connection.
    pub max_topics_per_conn: usize,
    /// Maximum concurrent WebSocket connections per client IP.
    pub max_conns_per_ip: u32,
    /// Sustained inbound frame rate per connection (token bucket refill per second).
    pub rate_limit_frames_per_sec: f64,
    /// Token bucket burst capacity per connection.
    pub rate_burst: f64,
    /// Connections with no inbound traffic (incl. pings) for this long are closed.
    pub idle_timeout_secs: u64,
    /// Server-side WebSocket ping interval; 0 disables. Keeps long-lived subscriber
    /// connections alive through proxies that drop idle tunnels (Cloudflare ~100 s,
    /// nginx default 60 s): clients auto-answer pongs, so BOTH directions stay active
    /// and no client-side keepalive logic is needed (browsers cannot send pings).
    pub keepalive_ping_secs: u64,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            bind: "localhost:7200".to_string(),
            frame_cap_bytes: 1024 * 1024,
            mailbox_ttl_secs: 300,
            mailbox_max_frames: 64,
            max_topics_per_conn: 8,
            max_conns_per_ip: 32,
            rate_limit_frames_per_sec: 20.0,
            rate_burst: 40.0,
            idle_timeout_secs: 900,
            keepalive_ping_secs: 30,
        }
    }
}

impl RelayConfig {
    /// Load from a TOML file. Unknown keys are rejected so a typo in an ops config
    /// fails loudly at startup instead of silently running with a default.
    pub fn from_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}
