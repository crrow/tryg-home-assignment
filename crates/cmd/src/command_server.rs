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

use clap::Args;
use snafu::{ResultExt, Whatever};
use tokio::signal;
use tracing::info;
use tryg_server::ServerBuilder;

#[derive(Debug, Clone, Args)]
#[command(
    name = "server",
    about = "Run the time series server",
    long_about = "Start the time series engine server with both gRPC and HTTP REST endpoints.

Examples:
  rsketch server
  rsketch server --port 8080
  rsketch server --host 0.0.0.0 --port 9090
  rsketch server --db-path /path/to/database"
)]
pub(crate) struct ServerArgs {
    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// gRPC port to bind to
    #[arg(short, long, default_value = "50051")]
    port: u16,

    /// HTTP REST port to bind to
    #[arg(long, default_value = "3000")]
    http_port: u16,

    /// Database path for data storage
    #[arg(long, default_value = "./data")]
    db_path: String,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

/// Run the server command
pub(crate) async fn run(args: ServerArgs) -> Result<(), Whatever> {
    info!(
        "Starting time series server on gRPC {}:{}, HTTP {}:{}",
        args.host, args.port, args.host, args.http_port
    );

    // Create server configuration
    let config = tryg_server::ServerConfig {
        host:      args.host.clone(),
        grpc_port: args.port,
        http_port: args.http_port,
        db_path:   args.db_path.clone(),
    };

    info!("Initializing database at: {}", args.db_path);

    // Create and start the server using the builder
    let server_builder =
        ServerBuilder::new(config).whatever_context("Failed to create server builder")?;

    let (mut grpc_handle, mut rest_handle) = server_builder
        .start()
        .await
        .whatever_context("Failed to start servers")?;

    // Wait for both servers to start
    grpc_handle
        .wait_for_start()
        .await
        .whatever_context("Failed to wait for gRPC server to start")?;

    rest_handle
        .wait_for_start()
        .await
        .whatever_context("Failed to wait for REST server to start")?;

    info!("Both gRPC and REST servers started successfully");
    info!("gRPC API available at: {}:{}", args.host, args.port);
    info!(
        "REST API available at: http://{}:{}",
        args.host, args.http_port
    );
    info!("REST endpoints:");
    info!("  GET  /api/v1/store/{{key}}?timestamp={{optional_timestamp}}");
    info!("  POST /api/v1/store");

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

    // Shutdown both servers
    info!("Shutting down servers...");
    grpc_handle.shutdown();
    rest_handle.shutdown();

    // Wait for both servers to stop
    grpc_handle
        .wait_for_stop()
        .await
        .whatever_context("Failed to wait for gRPC server to stop")?;

    rest_handle
        .wait_for_stop()
        .await
        .whatever_context("Failed to wait for REST server to stop")?;

    info!("Server shutdown complete");
    Ok(())
}
