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

pub mod grpc;
pub mod http;
pub mod services;

use std::sync::Arc;

use futures::future::join_all;
use snafu::{ResultExt, Snafu, Whatever};
use tokio::{sync::oneshot::Receiver, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tryg_db::DB;

use crate::{
    grpc::{GrpcServerConfig, start_grpc_server},
    http::{RestServerConfig, start_rest_server},
    services::{store_grpc::StoreSvc, store_rest::create_store_routes},
};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(transparent)]
    Network { source: NetworkError },
}

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum NetworkError {
    #[snafu(display("Failed to connect to {addr}"))]
    ConnectionError {
        addr:   String,
        #[snafu(source)]
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse address {addr}"))]
    ParseAddressError {
        addr:   String,
        #[snafu(source)]
        source: std::net::AddrParseError,
    },
}

type Result<T> = std::result::Result<T, Error>;

/// Configuration for the time series server
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host:      String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub db_path:   String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host:      "127.0.0.1".to_string(),
            grpc_port: 50051,
            http_port: 3000,
            db_path:   "./data".to_string(),
        }
    }
}

/// Builder for creating and configuring the time series server
///
/// This builder provides a fluent API for setting up both gRPC and HTTP servers
/// with shared database instance, making it easy to test and configure.
pub struct ServerBuilder {
    config: ServerConfig,
    db:     Arc<DB>,
}

impl ServerBuilder {
    /// Creates a new ServerBuilder with a database
    pub fn new(config: ServerConfig) -> std::result::Result<Self, Whatever> {
        let db = Arc::new(
            DB::options(&config.db_path)
                .open()
                .whatever_context("Failed to initialize database")?,
        );

        Ok(Self { config, db })
    }

    /// Starts both gRPC and HTTP servers
    ///
    /// Returns handles for both servers that can be used to manage their
    /// lifecycle
    pub async fn start(self) -> std::result::Result<(ServiceHandler, ServiceHandler), Whatever> {
        // Configure gRPC server
        let grpc_config = GrpcServerConfig::default()
            .with_address(format!("{}:{}", self.config.host, self.config.grpc_port));

        // Configure REST server
        let rest_config = RestServerConfig::builder()
            .bind_address(format!("{}:{}", self.config.host, self.config.http_port))
            .max_body_size(crate::http::DEFAULT_MAX_HTTP_BODY_SIZE)
            .enable_cors(true)
            .build();

        // Create services with shared database instance
        let store_grpc_service = Arc::new(StoreSvc::new(self.db.clone()));
        let store_rest_routes = create_store_routes(self.db);

        // Start both servers
        let grpc_handle = start_grpc_server(grpc_config, vec![store_grpc_service])
            .await
            .whatever_context("Failed to start gRPC server")?;

        let rest_handle = start_rest_server(rest_config, vec![store_rest_routes])
            .await
            .whatever_context("Failed to start REST server")?;

        Ok((grpc_handle, rest_handle))
    }
}

/// Handle for managing a running service: grpc or http.
///
/// This handle provides control over a running service, allowing you to:
/// - Wait for the service to start accepting connections
/// - Signal graceful shutdown
/// - Wait for the service to fully stop
/// - Check if the service task has completed
///
/// The handle uses a cancellation token for graceful shutdown and provides
/// async methods for coordinating server lifecycle events.
pub struct ServiceHandler {
    /// Join handle for the server task
    join_handle:        JoinHandle<()>,
    /// Token for signalling shutdown
    cancellation_token: CancellationToken,
    /// Receiver for server start notification
    started_rx:         Option<Receiver<()>>,
    /// Join handles for readiness reporting tasks
    reporter_handles:   Vec<JoinHandle<()>>,
}

impl ServiceHandler {
    /// Waits for the server to start accepting connections.
    ///
    /// This method blocks until the server has successfully bound to its
    /// configured address and is ready to accept gRPC requests.
    ///
    /// # Panics
    /// Panics if called more than once, as the start signal is consumed.
    pub async fn wait_for_start(&mut self) -> Result<()> {
        self.started_rx
            .take()
            .expect("Server start signal already consumed")
            .await
            .expect("Failed to receive server start signal");
        Ok(())
    }

    /// Waits for the server to completely stop.
    ///
    /// This method consumes the handle and blocks until the server task
    /// has finished executing. Use this after calling `shutdown()` to
    /// ensure clean termination.
    ///
    /// # Panics
    /// Panics if the server task panicked during execution.
    pub async fn wait_for_stop(self) -> Result<()> {
        let handles = self
            .reporter_handles
            .into_iter()
            .chain(std::iter::once(self.join_handle));
        join_all(handles).await;
        Ok(())
    }

    /// Signals the server to begin graceful shutdown.
    ///
    /// This method triggers the shutdown process but does not wait for
    /// completion. Use `wait_for_stop()` to wait for the server to fully stop.
    pub fn shutdown(&self) { self.cancellation_token.cancel(); }

    /// Checks if the server task has completed.
    ///
    /// Returns `true` if the server has finished running, either due to
    /// shutdown or an error condition.
    pub fn is_finished(&self) -> bool { self.join_handle.is_finished() }
}
