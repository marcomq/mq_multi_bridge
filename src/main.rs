//  mq-bridge-app
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge-app

mod config;
use config::load_config;
use std::net::SocketAddr;
use tracing::{info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use anyhow::Context;

use crate::config::AppConfig;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: AppConfig = load_config().context("Failed to load configuration")?;
    init_logging(&config);

    if config.routes.is_empty() {
        eprintln!("Warning: No routes configured. Application will not bridge any messages. Exiting.");
        warn!("No routes configured. Application will not bridge any messages. Exiting.");
        return Ok(());
    }

    run_app(config).await
}
fn init_logging(config: &AppConfig) {
    // --- 1. Initialize Logging ---
    // If the TOKIO_CONSOLE env var is set, initialize the console subscriber.
    // This is an exclusive choice, as the console subscriber is a logging layer.
    if std::env::var("TOKIO_CONSOLE").is_ok() {
        // console_subscriber::init();
        warn!("Tokio console subscriber not initialized. Cannot run `tokio-console` to connect.");
        return;
    }

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.log_level.clone()));

    let logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE) // Log entry and exit of spans
        .with_target(true);
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
    tracing::debug!("Logging initialized with level {} and logger {}", config.log_level, config.logger);
}

/// Waits for a platform-specific shutdown signal.
/// On Unix, this is SIGTERM. On other platforms, it's a future that never completes.
async fn platform_specific_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
                info!("SIGTERM received.");
            }
            Err(e) => {
                warn!("Failed to install SIGTERM handler: {}. This signal will be ignored.", e);
                // If we can't listen for the signal, pend forever.
                std::future::pending::<()>().await;
            }
        }
    }
    #[cfg(not(unix))]
    // On non-unix, ctrl_c is the primary mechanism. This future never completes.
    std::future::pending::<()>().await
}

async fn run_app(config: AppConfig) -> anyhow::Result<()> {
    info!("Starting MQ Multi Bridge application");

    if config.routes.is_empty() {
        warn!("No routes configured. Application will not bridge any messages. Exiting.");
        return Ok(());
    }

    // --- 2. Initialize Prometheus Metrics Exporter ---
    // We manually spawn the metrics server to be able to control its lifecycle.
    // The `install()` method spawns a task that we can't shut down.
    let metrics_task = if !config.metrics_addr.is_empty() {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        let addr: SocketAddr = config.metrics_addr.parse().context(format!(
            "Failed to parse metrics listen address: {}",
            config.metrics_addr
        ))?;
        let (recorder, server_future) = builder.with_http_listener(addr).build()?;
        let _ = metrics::set_default_local_recorder(&recorder);
        info!("Prometheus exporter listening on http://{}", addr);
        Some(tokio::spawn(server_future))
    } else {
        None
    };

    // --- 3. Run the bridge logic from the library ---
    let mut tasks = Vec::new();
    let mut shutdown_handles = Vec::new();

    for (name, route) in config.routes {
        let (task, shutdown) = route.run(&name);
        tasks.push(task);
        shutdown_handles.push(shutdown);
    }

    info!("Bridge running. Waiting for signal.");

    // --- 4. Wait for shutdown signal (Ctrl+C or SIGTERM) ---
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C (SIGINT) received.");
        },
        _ = platform_specific_shutdown() => {},
    }

    info!("Shutdown signal received. Broadcasting to all tasks...");

    for shutdown in shutdown_handles {
        shutdown.send(()).await.ok();
    }

    // Abort the metrics task if it's running. It doesn't support graceful shutdown.
    if let Some(task) = metrics_task {
        task.abort();
    }

    // Spawn a watchdog thread to ensure the process exits even if the runtime is blocked.
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(20));
        eprintln!("Shutdown watchdog: Main thread seems to be blocked. Forcefully exiting after 20 seconds.");
        std::process::exit(1); // Exit with a non-zero code to indicate an issue.
    });

    let wait_for_tasks = async {
        for task in &mut tasks {
            if let Err(e) = task.await {
                // It's normal for a task to be cancelled during shutdown.
                // We only warn if it's another type of error.
                if !e.is_cancelled() {
                    warn!("Route task join error during shutdown: {}", e);
                }
            }
        }
    };

    if let Err(_) = tokio::time::timeout(std::time::Duration::from_secs(10), wait_for_tasks).await {
        warn!("Shutdown timed out after 10 seconds. Aborting all tasks.");
        for task in tasks {
            task.abort();
        }
    }
    Ok(())
}
