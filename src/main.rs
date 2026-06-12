// Binary entry point: load the optional TOML config, start the relay, run until
// Ctrl-C. All real logic lives in the library (shared with the integration tests).

use phantasma_link_relay::config::RelayConfig;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Usage: phantasma-link-relay [config.toml]. No file = built-in defaults
    // (localhost bind), which is the correct posture behind the reverse proxy.
    let config = match std::env::args().nth(1) {
        Some(path) => {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("cannot read config {path}: {err}"));
            RelayConfig::from_toml(&text)
                .unwrap_or_else(|err| panic!("invalid config {path}: {err}"))
        }
        None => RelayConfig::default(),
    };

    let (_addr, server) = phantasma_link_relay::start(config)
        .await
        .expect("relay failed to bind");

    tokio::select! {
        _ = server => {}
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown requested");
        }
    }
}
