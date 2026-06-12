// Topic hub: the relay's whole brain. Routes a publish to every OTHER subscriber of
// the topic, and when nobody is listening holds the frame in a per-topic mailbox up to
// the TTL (spec section 18: a deeplink-woken wallet must be able to fetch a request
// published moments before it connected). The hub is deliberately dumb: no crypto, no
// chain logic, no payload inspection.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

/// Outbound queue handle of one connection. Bounded: a subscriber that stops reading
/// gets dropped (slow-consumer policy) instead of growing relay memory.
pub type FrameSender = mpsc::Sender<String>;

#[derive(Default)]
struct TopicState {
    /// conn id -> outbound queue of that subscriber.
    subscribers: HashMap<u64, FrameSender>,
    /// Undelivered `deliver` frames waiting for a subscriber, newest at the back.
    mailbox: VecDeque<MailboxEntry>,
}

struct MailboxEntry {
    expires_at: Instant,
    frame: String,
}

pub struct Hub {
    topics: Mutex<HashMap<String, TopicState>>,
    mailbox_ttl: Duration,
    mailbox_max: usize,
}

impl Hub {
    pub fn new(mailbox_ttl: Duration, mailbox_max: usize) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            mailbox_ttl,
            mailbox_max,
        }
    }

    /// Register a subscriber and immediately flush any unexpired mailbox backlog to it
    /// (the wake-deeplink flow: publish happens BEFORE the wallet finishes connecting).
    pub fn subscribe(&self, topic: &str, conn_id: u64, sender: FrameSender) {
        let mut topics = self.topics.lock().expect("hub lock");
        let state = topics.entry(topic.to_string()).or_default();
        let now = Instant::now();
        // Flush, dropping entries that expired while nobody listened.
        for entry in state.mailbox.drain(..) {
            if entry.expires_at > now {
                // A full/closed queue here means the brand-new subscriber is already
                // broken; the frame is lost for it, which equals the no-subscriber case.
                let _ = sender.try_send(entry.frame);
            }
        }
        state.subscribers.insert(conn_id, sender);
    }

    pub fn unsubscribe(&self, topic: &str, conn_id: u64) {
        let mut topics = self.topics.lock().expect("hub lock");
        if let Some(state) = topics.get_mut(topic) {
            state.subscribers.remove(&conn_id);
            if state.subscribers.is_empty() && state.mailbox.is_empty() {
                topics.remove(topic);
            }
        }
    }

    /// Route one already-serialized `deliver` frame. Returns the number of subscribers
    /// it was handed to; 0 means it went to the mailbox.
    pub fn publish(&self, topic: &str, from_conn: u64, deliver_frame: String) -> usize {
        let mut topics = self.topics.lock().expect("hub lock");
        let state = topics.entry(topic.to_string()).or_default();

        // Send to every subscriber except the publisher itself (spec: "every other
        // subscriber"). Slow consumers (full queue) and dead queues are dropped from
        // the topic so they cannot absorb future traffic.
        let mut delivered = 0usize;
        state.subscribers.retain(|&conn_id, sender| {
            if conn_id == from_conn {
                return true;
            }
            match sender.try_send(deliver_frame.clone()) {
                Ok(()) => {
                    delivered += 1;
                    true
                }
                Err(_) => false,
            }
        });

        if delivered == 0 {
            // Nobody (else) is listening: hold for late subscribers, bounded both by
            // TTL and by depth (oldest dropped first; newest state matters most).
            if state.mailbox.len() >= self.mailbox_max {
                state.mailbox.pop_front();
            }
            state.mailbox.push_back(MailboxEntry {
                expires_at: Instant::now() + self.mailbox_ttl,
                frame: deliver_frame,
            });
        }
        delivered
    }

    /// Remove a closed connection from every topic it was subscribed to.
    pub fn drop_connection(&self, conn_id: u64, topics_of_conn: &[String]) {
        for topic in topics_of_conn {
            self.unsubscribe(topic, conn_id);
        }
    }

    /// Periodic sweep: expire mailbox entries and forget empty topics, so abandoned
    /// pairings cannot accumulate memory.
    pub fn prune(&self) {
        let now = Instant::now();
        let mut topics = self.topics.lock().expect("hub lock");
        topics.retain(|_, state| {
            state.mailbox.retain(|entry| entry.expires_at > now);
            !state.subscribers.is_empty() || !state.mailbox.is_empty()
        });
    }

    /// Test/observability helper: current number of live topics.
    pub fn topic_count(&self) -> usize {
        self.topics.lock().expect("hub lock").len()
    }
}
