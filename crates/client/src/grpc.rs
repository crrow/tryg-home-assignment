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

use snafu::{ResultExt, ensure};
use tonic::transport::Channel;
use tracing::{debug, info};
use tryg_api::pb::store::v1::{GetRequest, ListRequest, SetRequest, store_client::StoreClient};

use crate::types::{ClientError, ClientResult, ListResponse, Series};

/// gRPC client for communicating with the RSketch timeseries server
///
/// This client provides a high-level interface for storing and retrieving
/// time series data using the gRPC protocol.
///
/// # Examples
///
/// ```rust,no_run
/// use rsketch_client::{GrpcClient, Series};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Connect to the server
///     let mut client = GrpcClient::connect("http://localhost:9090").await?;
///
///     // Store a data point
///     let series = Series {
///         key:       "temperature_sensor_1".to_string(),
///         value:     "23.5".to_string(),
///         timestamp: 1641024000000,
///     };
///     client.set(series).await?;
///
///     // Retrieve the data point
///     let result = client.get("temperature_sensor_1", None).await?;
///     println!("Retrieved: {} = {}", result.key, result.value);
///
///     // List all stored data
///     let list_result = client.list(None, Some(10)).await?;
///     println!("Found {} series", list_result.series.len());
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct GrpcClient {
    /// The underlying gRPC client
    client: StoreClient<Channel>,
}

impl GrpcClient {
    /// Connects to a gRPC server at the specified endpoint
    ///
    /// # Arguments
    /// * `endpoint` - The server endpoint (e.g., "http://localhost:9090")
    ///
    /// # Returns
    /// A connected gRPC client instance
    ///
    /// # Errors
    /// Returns a `ClientError::Connection` if the connection fails
    pub async fn connect<S: AsRef<str>>(endpoint: S) -> ClientResult<Self> {
        let endpoint = endpoint.as_ref();
        info!("Connecting to gRPC server at {}", endpoint);

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ClientError::Connection {
                message: format!("Invalid endpoint '{endpoint}': {e}"),
            })?
            .connect()
            .await
            .map_err(|e| ClientError::Connection {
                message: format!("Failed to connect to '{endpoint}': {e}"),
            })?;

        let client = StoreClient::new(channel);

        info!("Successfully connected to gRPC server");
        Ok(Self { client })
    }

    /// Retrieves a value by key, optionally at a specific timestamp
    ///
    /// If no timestamp is provided, returns the latest value.
    /// If a timestamp is provided, returns the most recent value
    /// that is not newer than the specified timestamp.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    /// * `timestamp` - Optional timestamp in nanoseconds since Unix epoch
    ///
    /// # Returns
    /// The series data point if found
    ///
    /// # Errors
    /// Returns a `ClientError::Grpc` if the operation fails
    pub async fn get<K: Into<String>>(
        &mut self,
        key: K,
        timestamp: Option<i64>,
    ) -> ClientResult<Series> {
        let key = key.into();
        debug!(
            "Getting value for key: {} at timestamp: {:?}",
            key, timestamp
        );

        let request = GetRequest {
            key: key.clone(),
            timestamp,
        };

        let response = self
            .client
            .get(request)
            .await
            .context(crate::types::GrpcSnafu)?;

        let series = response
            .into_inner()
            .series
            .ok_or_else(|| ClientError::ServerError {
                message: format!("Server returned empty response for key '{key}'"),
            })?;

        debug!("Successfully retrieved value for key: {}", key);
        Ok(series.into())
    }

    /// Stores a key-value pair with a timestamp
    ///
    /// # Arguments
    /// * `series` - The series data point to store
    ///
    /// # Returns
    /// Unit on success
    ///
    /// # Errors
    /// Returns a `ClientError` if the operation fails
    pub async fn set(&mut self, series: Series) -> ClientResult<()> {
        debug!(
            "Setting value for key: {} = {} at timestamp: {}",
            series.key, series.value, series.timestamp
        );

        // Validate timestamp
        ensure!(
            series.timestamp >= 0,
            crate::types::InvalidArgumentSnafu {
                message: "Timestamp must be non-negative",
            }
        );

        let request = SetRequest {
            series: Some(series.into()),
        };

        self.client
            .set(request)
            .await
            .context(crate::types::GrpcSnafu)?;

        debug!("Successfully stored value");
        Ok(())
    }

    /// Lists all stored keys with their latest values
    ///
    /// # Arguments
    /// * `key_prefix` - Optional key prefix to filter results
    /// * `limit` - Optional limit on number of results (default: 100, max:
    ///   1000)
    ///
    /// # Returns
    /// A list response containing the series and pagination information
    ///
    /// # Errors
    /// Returns a `ClientError` if the operation fails
    pub async fn list(
        &mut self,
        key_prefix: Option<String>,
        limit: Option<i32>,
    ) -> ClientResult<ListResponse> {
        debug!(
            "Listing keys with prefix: {:?}, limit: {:?}",
            key_prefix, limit
        );

        // Validate limit
        if let Some(limit_val) = limit {
            ensure!(
                limit_val > 0 && limit_val <= 1000,
                crate::types::InvalidArgumentSnafu {
                    message: "Limit must be between 1 and 1000",
                }
            );
        }

        let request = ListRequest { key_prefix, limit };

        let response = self
            .client
            .list(request)
            .await
            .context(crate::types::GrpcSnafu)?;

        let list_response = response.into_inner();
        debug!(
            "Successfully listed {} series, has_more: {}",
            list_response.series.len(),
            list_response.has_more
        );

        Ok(list_response.into())
    }

    /// Gets the current timestamp in nanoseconds since Unix epoch
    ///
    /// This is a utility function that can be used to generate timestamps
    /// for storing data points.
    pub fn current_timestamp() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }
}
