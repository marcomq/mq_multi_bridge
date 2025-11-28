//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

// Use the library crate
use anyhow::Context;
use mq_multi_bridge::config;
use mq_multi_bridge::config::load_config;
use std::net::SocketAddr;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: config::Config = load_config().context("Failed to load configuration")?;

    // --- 1. Initialize Logging ---
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("mq_multi_bridge={}", config.log_level)));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE) // Log entry and exit of spans
        .with_target(true)
        .json() // Output logs in JSON format
        .init();

    info!("Starting MQ Multi Bridge application");

    // --- 2. Initialize Prometheus Metrics Exporter ---
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let addr: SocketAddr = "0.0.0.0:9090".parse()?;
    builder.with_http_listener(addr).install()?;
    info!("Prometheus exporter listening on {}", addr);

    // --- 3. Create Shutdown Signal Channel ---
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    // --- 3. Run the bridge logic from the library ---
    let mut bridge_handle = tokio::spawn(mq_multi_bridge::run(config, shutdown_rx));

    // --- 4. Wait for shutdown signal or task completion ---
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received. Broadcasting to all tasks...");
            // Send shutdown signal
            shutdown_tx.send(()).context("Failed to send shutdown signal")?;
            info!("Waiting for bridge tasks to complete...");
        }
        res = &mut bridge_handle => {
             match res {
                Ok(Ok(_)) => warn!("Bridge task finished unexpectedly without an error."),
                Ok(Err(e)) => error!("Bridge task finished with an error: {}", e),
                Err(e) => error!("Bridge task panicked: {}", e),
            }
        }
    }

    // Wait for the bridge to finish shutting down after a Ctrl+C
    let _ = bridge_handle.await;
    info!("Bridge has shut down gracefully.");
    Ok(())
}
