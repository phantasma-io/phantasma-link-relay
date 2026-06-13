# phantasma-link-relay

The Phantasma Link v5 relay: a small, E2E-blind publish/subscribe server over
WebSocket. It carries the protocol's "fat" traffic that cannot ride a deeplink URL
(large transactions, cross-device sessions, wallet-to-dApp events) while knowing
nothing about its content.

Design source: Phantasma Link v5 specification, section 18 (relay protocol) and
section 6.4 (transport role).

## Properties

- **E2E-blind**: every `payload` is opaque ciphertext sealed by the dApp/wallet with
  their NaCl session key. The relay holds no keys, decrypts nothing, and contains no
  Phantasma SDK or chain logic.
- **Topic routing**: a `publish` on a topic is forwarded to every OTHER subscriber of
  that topic. The topic id is a bearer capability minted by the SDK (32 random bytes,
  base64url): knowing it means being part of the session.
- **Mailbox**: when a topic has no subscribers, frames are held up to a TTL
  (default 300 s, depth-capped) so a deeplink-woken wallet can fetch a request that
  was published moments before it connected.
- **Abuse bounds**: per-frame size cap (default 1 MiB; larger messages are chunked by
  the clients), per-connection rate limit and topic cap, per-IP connection cap, idle
  timeout, periodic mailbox sweep.
- **Plain WS on localhost**: TLS terminates at the reverse proxy of
  link.phantasma.info (see `hosting/` for the nginx vhost and static site files).

## Wire format

JSON text frames over `GET /relay` (WebSocket upgrade). `GET /healthz` answers `ok`.

Client to relay:

```json
{ "op": "subscribe",   "topic": "<topic>" }
{ "op": "unsubscribe", "topic": "<topic>" }
{ "op": "publish",     "topic": "<topic>", "id": "<optional>", "payload": <opaque JSON> }
```

Relay to client:

```json
{ "op": "deliver", "topic": "<topic>", "id": "<publisher id if any>", "payload": <opaque JSON> }
{ "op": "ack",     "topic": "<topic>", "id": "<publisher id if any>" }
{ "op": "error",   "code": "<short code>", "message": "<human text>", "id": "<if known>" }
```

- `ack` means the publish was accepted: delivered now, or mailboxed for the TTL. The
  relay never reports HOW many peers received a frame; that is session-layer business.
- Error codes: `bad_frame`, `bad_topic`, `frame_too_large`, `rate_limited`,
  `topic_limit`. Protocol violations (`bad_frame`, `frame_too_large`, `rate_limited`,
  invalid topic) close the connection after the error frame; `topic_limit` does not.
- Keepalive: standard WebSocket pings (answered automatically) also reset the idle
  timer.

## Run

```sh
cargo run --release                      # built-in defaults (localhost:7200)
cargo run --release -- config.toml       # with a config file
```

See `config.example.toml` for every knob and its default. Logging via `RUST_LOG`
(default `info`).

## Develop

```sh
just f        # cargo fmt
just t        # cargo test (integration tests run over real WebSockets)
just clippy   # cargo clippy --all-targets -- -D warnings
just build    # release build
```
