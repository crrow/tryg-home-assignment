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

use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use tracing::{debug, info};
use url::Url;

use crate::types::{ClientError, ClientResult, ListResponse, Series};

/// REST client for communicating with the RSketch timeseries server
///
/// This client provides a high-level interface for storing and retrieving
/// time series data using the HTTP REST API.
///
/// # Examples
///
/// ```rust,no_run
/// use rsketch_client::{RestClient, Series};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create a client
///     let client = RestClient::new("http://localhost:8080")?;
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
pub struct RestClient {
    /// The HTTP client
    client:   Client,
    /// Base URL for the server
    base_url: Url,
}

/// Request structure for set operations
#[derive(Debug, Serialize)]
struct SetRequest {
    series: Series,
}

/// Response structure for get operations
#[derive(Debug, Deserialize)]
struct GetResponse {
    series: Series,
}

/// Error response structure from the server
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error:   String,
    message: String,
}

impl RestClient {
    /// Creates a new REST client for the specified server
    ///
    /// # Errors
    /// Returns a `ClientError` if the base URL is invalid
    pub fn new<S: AsRef<str>>(base_url: S) -> ClientResult<Self> {
        info!("Creating REST client for {}", base_url.as_ref());

        // Create HTTP client with no proxy to avoid proxy issues with localhost
        let client = Client::builder()
            .no_proxy()
            .build()
            .context(crate::types::HttpSnafu)?;

        let base_url = Url::parse(base_url.as_ref()).context(crate::types::InvalidUrlSnafu)?;

        info!("Successfully created REST client");
        Ok(Self { client, base_url })
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
    /// Returns a `ClientError` if the operation fails
    pub async fn get<K: Into<String>>(
        &self,
        key: K,
        timestamp: Option<i64>,
    ) -> ClientResult<Series> {
        let key = key.into();
        debug!(
            "Getting value for key: {} at timestamp: {:?}",
            key, timestamp
        );

        let mut url = self
            .base_url
            .join(&format!("api/v1/store/{}", urlencoding::encode(&key)))
            .context(crate::types::InvalidUrlSnafu)?;

        // Add timestamp query parameter if provided
        if let Some(ts) = timestamp {
            url.query_pairs_mut()
                .append_pair("timestamp", &ts.to_string());
        }

        let response = self
            .client
            .get(url)
            .send()
            .await
            .context(crate::types::HttpSnafu)?;

        if response.status().is_success() {
            let get_response: GetResponse =
                response.json().await.context(crate::types::HttpSnafu)?;

            debug!("Successfully retrieved value for key: {}", key);
            Ok(get_response.series)
        } else if response.status() == 404 {
            Err(ClientError::ServerError {
                message: format!("Key '{key}' not found"),
            })
        } else {
            // Try to parse error response
            let status = response.status();
            match response.json::<ErrorResponse>().await {
                Ok(error_resp) => Err(ClientError::ServerError {
                    message: format!("{}: {}", error_resp.error, error_resp.message),
                }),
                Err(_) => Err(ClientError::ServerError {
                    message: format!("Server returned status: {status}"),
                }),
            }
        }
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
    pub async fn set(&self, series: Series) -> ClientResult<()> {
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

        let url = self
            .base_url
            .join("api/v1/store")
            .context(crate::types::InvalidUrlSnafu)?;

        let request_body = SetRequest { series };

        debug!("Sending POST request to: {}", url);
        debug!("Request body: {:?}", request_body);

        let response = self
            .client
            .post(url)
            .json(&request_body)
            .send()
            .await
            .context(crate::types::HttpSnafu)?;

        debug!("Response status: {}", response.status());

        if response.status().is_success() {
            debug!("Successfully stored value");
            Ok(())
        } else {
            // Try to parse error response
            let status = response.status();
            match response.json::<ErrorResponse>().await {
                Ok(error_resp) => Err(ClientError::ServerError {
                    message: format!("{}: {}", error_resp.error, error_resp.message),
                }),
                Err(_) => Err(ClientError::ServerError {
                    message: format!("Server returned status: {status}"),
                }),
            }
        }
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
        &self,
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

        let mut url = self
            .base_url
            .join("api/v1/store/list")
            .context(crate::types::InvalidUrlSnafu)?;

        // Add query parameters
        {
            let mut query_pairs = url.query_pairs_mut();
            if let Some(prefix) = &key_prefix {
                query_pairs.append_pair("key_prefix", prefix);
            }
            if let Some(limit_val) = limit {
                query_pairs.append_pair("limit", &limit_val.to_string());
            }
        }

        debug!("Sending GET request to: {}", url);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .context(crate::types::HttpSnafu)?;

        debug!("List response status: {}", response.status());

        if response.status().is_success() {
            let list_response: ListResponse =
                response.json().await.context(crate::types::HttpSnafu)?;

            debug!(
                "Successfully listed {} series, has_more: {}",
                list_response.series.len(),
                list_response.has_more
            );

            Ok(list_response)
        } else {
            // Try to parse error response
            let status = response.status();
            match response.json::<ErrorResponse>().await {
                Ok(error_resp) => Err(ClientError::ServerError {
                    message: format!("{}: {}", error_resp.error, error_resp.message),
                }),
                Err(_) => Err(ClientError::ServerError {
                    message: format!("Server returned status: {status}"),
                }),
            }
        }
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
