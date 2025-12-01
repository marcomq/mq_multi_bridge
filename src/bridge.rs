//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

use crate::config::Config;
use crate::route_runner::RouteRunner;
use anyhow::Result;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

pub struct Bridge {
    config: Config,
    shutdown_rx: watch::Receiver<()>,
    shutdown_tx: watch::Sender<()>,
}

impl Bridge {
    /// Creates a new Bridge from a configuration object.
    pub fn new(config: Config) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        Self {
            config,
            shutdown_rx,
            shutdown_tx,
        }
    }

    /// Returns a `watch::Sender` that can be used to trigger a graceful shutdown of the bridge.
    pub fn get_shutdown_handle(&self) -> watch::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Runs all configured routes and returns a `JoinHandle` for the main bridge task.
    /// The bridge will run until all routes have completed (e.g., file EOF) or a shutdown
    /// signal is received.
    pub fn run(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            info!("Bridge starting up...");
            let mut route_tasks = JoinSet::new();

            for (name, route) in self.config.routes.iter() {
                let route_runner = RouteRunner::new(
                    name.clone(),
                    route.clone(),
                    self.config.clone(),
                    self.shutdown_rx.clone(),
                );
                route_tasks.spawn(route_runner.run());
            }

            if route_tasks.is_empty() {
                warn!("No routes configured or initialized. Bridge will shut down.");
                return Ok(());
            }

            // Wait for either a shutdown signal or for all route tasks to complete.
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    info!("Global shutdown signal received. Draining all routes.");
                }
                _ = async { while route_tasks.join_next().await.is_some() {} } => {
                    info!("All routes have completed their work. Bridge shutting down.");
                }
            }

            // Ensure all tasks are finished.
            route_tasks.shutdown().await;
            info!("Bridge has shut down.");
            Ok(())
        })
    }
}
