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

use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// Common result type for client operations
pub type ClientResult<T> = Result<T, ClientError>;

/// Client error types
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ClientError {
    /// gRPC transport or protocol error
    #[snafu(display("gRPC error: {source}"))]
    Grpc { source: tonic::Status },

    /// HTTP request error
    #[snafu(display("HTTP error: {source}"))]
    Http { source: reqwest::Error },

    /// URL parsing error
    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    /// JSON serialization/deserialization error
    #[snafu(display("JSON error: {source}"))]
    Json { source: serde_json::Error },

    /// Invalid argument error
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    /// Server returned an error
    #[snafu(display("Server error: {message}"))]
    ServerError { message: String },

    /// Connection error
    #[snafu(display("Connection error: {message}"))]
    Connection { message: String },
}

/// Time series data point
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Series {
    /// The key identifier for this series
    pub key:       String,
    /// The value for this data point
    pub value:     String,
    /// Timestamp in nanoseconds since Unix epoch
    pub timestamp: i64,
}

/// Response from list operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    /// List of series data points
    pub series:   Vec<Series>,
    /// Whether there are more results available
    pub has_more: bool,
}

impl From<rsketch_api::pb::store::v1::Series> for Series {
    fn from(grpc_series: rsketch_api::pb::store::v1::Series) -> Self {
        Series {
            key:       grpc_series.key,
            value:     grpc_series.value,
            timestamp: grpc_series.timestamp,
        }
    }
}

impl From<Series> for rsketch_api::pb::store::v1::Series {
    fn from(series: Series) -> Self {
        rsketch_api::pb::store::v1::Series {
            key:       series.key,
            value:     series.value,
            timestamp: series.timestamp,
        }
    }
}

impl From<rsketch_api::pb::store::v1::ListResponse> for ListResponse {
    fn from(grpc_response: rsketch_api::pb::store::v1::ListResponse) -> Self {
        ListResponse {
            series:   grpc_response.series.into_iter().map(Series::from).collect(),
            has_more: grpc_response.has_more,
        }
    }
}
