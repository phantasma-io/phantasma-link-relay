// Relay wire frames (spec section 18): JSON text messages over the WebSocket,
// discriminated by `op`. The `payload` is OPAQUE to the relay: it is the NaCl-sealed
// envelope produced by the SDK/wallet (the relay is E2E-blind and never inspects it).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Frames a client may send to the relay.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientFrame {
    Subscribe {
        topic: String,
    },
    Unsubscribe {
        topic: String,
    },
    Publish {
        topic: String,
        /// Publisher-chosen correlation id, echoed in the ack and in the deliver
        /// frame so receivers can de-duplicate redeliveries.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        payload: Value,
    },
}

/// Frames the relay sends to clients.
#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ServerFrame<'a> {
    /// A publish forwarded to the other subscribers of the topic.
    Deliver {
        topic: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<&'a str>,
        payload: &'a Value,
    },
    /// Confirms a publish was accepted (delivered now or mailboxed for the TTL).
    Ack {
        topic: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<&'a str>,
    },
    /// Protocol or limit violation. The connection MAY be closed right after.
    Error {
        code: &'a str,
        message: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<&'a str>,
    },
}

impl ServerFrame<'_> {
    pub fn to_json(&self) -> String {
        // Serialization of these shapes cannot fail (no maps with non-string keys).
        serde_json::to_string(self).expect("server frame serializes")
    }
}

/// Topics are opaque bearer capabilities minted by the SDK (32 random bytes,
/// base64url). The relay only bounds them: non-empty, sane length, no whitespace.
pub fn topic_is_valid(topic: &str) -> bool {
    !topic.is_empty() && topic.len() <= 128 && !topic.chars().any(char::is_whitespace)
}
