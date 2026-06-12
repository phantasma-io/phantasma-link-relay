# Multi-stage build mirroring the pha trading-bot images: rust builder, slim runtime,
# non-root user. The relay is a single static-config binary; the config is mounted.
FROM rust:1.91 AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 relay
COPY --from=builder /src/target/release/phantasma-link-relay /usr/local/bin/phantasma-link-relay
USER relay
# The container binds 0.0.0.0 via the mounted config (nginx on the host reaches it
# through the published/forwarded port); never publish this port publicly.
EXPOSE 7200
ENTRYPOINT ["/usr/local/bin/phantasma-link-relay"]
CMD ["/etc/phantasma-link-relay/config.toml"]
