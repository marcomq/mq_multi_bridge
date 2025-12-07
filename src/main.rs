//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge
use mq_multi_bridge::{
    bridge::Bridge,
    config::{load_config, Config},
};
use std::net::SocketAddr;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use anyhow::Context;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: Config = load_config().context("Failed to load configuration")?;
    init_logging(&config);

    if config.routes.is_empty() {
        warn!("No routes configured. Application will not bridge any messages. Exiting.");
        return Ok(());
    }

    run_app(config).await
}
fn init_logging(config: &Config) {
    // --- 1. Initialize Logging ---
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("mq_multi_bridge={}", config.log_level)));

    let logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE) // Log entry and exit of spans
        .with_target(true);
    dbg!(&config.logger);
    match config.logger.as_str() {
        "json" => {
            logger.json().init();
        }
        "plain" => {
            logger.init();
        }
        _ => {
            logger.init();
        }
    }
}

async fn run_app(config: Config) -> anyhow::Result<()> {
    info!("Starting MQ Multi Bridge application");

    if config.routes.is_empty() {
        warn!("No routes configured. Application will not bridge any messages. Exiting.");
        return Ok(());
    }

    // --- 2. Initialize Prometheus Metrics Exporter ---
    if config.metrics.enabled {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        let addr: SocketAddr = config.metrics.listen_address.parse().context(format!(
            "Failed to parse metrics listen address: {}",
            config.metrics.listen_address
        ))?;
        builder.with_http_listener(addr).install()?;
        info!("Prometheus exporter listening on http://{}", addr);
    }

    // --- 3. Run the bridge logic from the library ---
    let mut bridge = Bridge::new(config);
    let shutdown_tx = bridge.get_shutdown_handle();
    let bridge_handle = bridge.run();

    // --- 4. Wait for shutdown signal (Ctrl+C or SIGTERM) or task completion ---
    tokio::select! {
        // Handle Ctrl+C (SIGINT)
        Ok(()) = tokio::signal::ctrl_c() => {
            info!("Ctrl+C (SIGINT) received.");
        },
        // Handle SIGTERM (common for service shutdowns)
        _ = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler").recv().await;
            }
        } => {
            info!("SIGTERM received.");
        },
        res = bridge_handle => {
             match res {
                Ok(Ok(_)) => info!("Bridge task finished."),
                Ok(Err(e)) => error!("Bridge task finished with an error: {}", e),
                Err(e) => error!("Bridge task panicked: {}", e),
            }
            return Ok(()); // Exit if the bridge task finishes on its own
        }
    }

    info!("Shutdown signal received. Broadcasting to all tasks...");
    if shutdown_tx.send(()).is_err() {
        warn!("Could not send shutdown signal, bridge already stopped.");
    }

    info!("Waiting up to 200ms for graceful shutdown...");
    Ok(())
}
