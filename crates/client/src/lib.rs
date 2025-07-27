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

//! RSketch Client Library
//!
//! This library provides both gRPC and REST clients for communicating with
//! the RSketch timeseries database server.
//!
//! # Features
//!
//! - **gRPC Client**: High-performance binary protocol client
//! - **REST Client**: HTTP-based client with JSON payloads
//! - **Unified Interface**: Both clients provide the same API surface
//! - **Comprehensive Error Handling**: Using snafu for structured error types
//! - **Async/Await Support**: Full tokio compatibility
//!
//! # Examples
//!
//! ## Using the gRPC client
//!
//! ```rust,no_run
//! use rsketch_client::{GrpcClient, Series};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = GrpcClient::connect("http://localhost:9090").await?;
//!
//!     // Set a value
//!     let series = Series {
//!         key:       "sensor_01".to_string(),
//!         value:     "23.5".to_string(),
//!         timestamp: 1641024000000,
//!     };
//!     client.set(series).await?;
//!
//!     // Get the value back
//!     let result = client.get("sensor_01", None).await?;
//!     println!("Value: {}", result.value);
//!
//!     // List all values
//!     let list_result = client.list(None, None).await?;
//!     for series in list_result.series {
//!         println!("{}: {}", series.key, series.value);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Using the REST client
//!
//! ```rust,no_run
//! use rsketch_client::{RestClient, Series};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = RestClient::new("http://localhost:8080")?;
//!
//!     // Set a value
//!     let series = Series {
//!         key:       "sensor_01".to_string(),
//!         value:     "23.5".to_string(),
//!         timestamp: 1641024000000,
//!     };
//!     client.set(series).await?;
//!
//!     // Get the value back
//!     let result = client.get("sensor_01", None).await?;
//!     println!("Value: {}", result.value);
//!
//!     // List all values
//!     let list_result = client.list(None, None).await?;
//!     for series in list_result.series {
//!         println!("{}: {}", series.key, series.value);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Protocol Selection
//!
//! Choose between gRPC and REST based on your needs:
//!
//! - **Use gRPC** for:
//!   - High-performance applications
//!   - Service-to-service communication
//!   - Type-safe protobuf schemas
//!   - Streaming (if supported in future versions)
//!
//! - **Use REST** for:
//!   - Web applications
//!   - Simple HTTP tooling integration
//!   - Debugging with curl/browser
//!   - Cross-language compatibility

pub mod grpc;
pub mod rest;
pub mod types;

// Re-export the main client types
pub use grpc::GrpcClient;
pub use rest::RestClient;
pub use types::{ClientError, ClientResult, ListResponse, Series};
