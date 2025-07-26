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
use rsketch_db::DB;
use rsketch_server::{
    grpc::{GrpcServerConfig, start_grpc_server},
    http::{RestServerConfig, start_rest_server},
    services::{store_grpc::StoreSvc, store_rest::create_store_routes},
};
use snafu::{ResultExt, Whatever};
use tokio::signal;
use tracing::info;

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

    // Initialize the database
    info!("Initializing database at: {}", args.db_path);
    let db = Arc::new(
        DB::options(&args.db_path)
            .open()
            .whatever_context("Failed to initialize database")?,
    );
    info!("Database initialized successfully");

    // Configure gRPC server
    let grpc_config =
        GrpcServerConfig::default().with_address(format!("{}:{}", args.host, args.port));

    // Configure REST server
    let rest_config = RestServerConfig::builder()
        .bind_address(format!("{}:{}", args.host, args.http_port))
        .max_body_size(rsketch_server::http::DEFAULT_MAX_HTTP_BODY_SIZE)
        .enable_cors(true)
        .build();

    // Create services with shared database instance
    let store_grpc_service = Arc::new(StoreSvc::new(db.clone()));
    let store_rest_routes = create_store_routes(db.clone());

    // Start both servers
    info!("Starting gRPC server on {}:{}", args.host, args.port);
    let mut grpc_handle = start_grpc_server(grpc_config, vec![store_grpc_service])
        .await
        .whatever_context("Failed to start gRPC server")?;

    info!("Starting REST server on {}:{}", args.host, args.http_port);
    let mut rest_handle = start_rest_server(rest_config, vec![store_rest_routes])
        .await
        .whatever_context("Failed to start REST server")?;

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

    // Close the database
    info!("Closing database...");
    db.close().whatever_context("Failed to close database")?;

    info!("Server shutdown complete");
    Ok(())
}
