// Copyright 2025 Crrow
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use clap::Args;
use rsketch_server::services::store::StoreSvc;
use snafu::{ResultExt, Whatever};
use tokio::signal;
use tracing::info;

#[derive(Debug, Clone, Args)]
#[command(
    name = "server",
    about = "Run the time series server",
    long_about = "Start the time series engine server.

Examples:
  rsketch server
  rsketch server --port 8080
  rsketch server --host 0.0.0.0 --port 9090"
)]
pub(crate) struct ServerArgs {
    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to bind to
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

/// Run the server command
pub(crate) async fn run(args: ServerArgs) -> Result<(), Whatever> {
    info!("Starting time series server on {}:{}", args.host, args.port);
    let grpc_config = rsketch_server::grpc::GrpcServerConfig::default()
        .with_address(format!("{}:{}", args.host, args.port));

    // Spawn the main server task
    let mut server_handle =
        rsketch_server::grpc::start_grpc_server(grpc_config, vec![Arc::new(StoreSvc::new())])
            .await
            .whatever_context("Failed to start gRPC server")?;

    server_handle
        .wait_for_start()
        .await
        .whatever_context("Failed to wait for server to start")?;

    // Handle shutdown signals
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
        _ = async {
            if let Ok(mut sigterm) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
                sigterm.recv().await;
            }
        } => {
            info!("Received SIGTERM, shutting down...");
        }
    }
    server_handle.shutdown();
    server_handle
        .wait_for_stop()
        .await
        .whatever_context("Failed to wait for server to stop")?;

    info!("Server shutdown complete");
    Ok(())
}
