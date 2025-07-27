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

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use rsketch_db::DB;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

/// REST Store service implementation
///
/// This service exposes the rsketch-db functionality over HTTP REST endpoints,
/// providing get and set operations for time series data storage and retrieval.
#[derive(Debug, Clone)]
pub struct StoreRestService {
    /// Database instance for data storage and retrieval
    db: Arc<DB>,
}

/// Series data structure for JSON serialization/deserialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub key:       String,
    pub value:     String,
    pub timestamp: i64,
}

/// Query parameters for get requests
#[derive(Debug, Deserialize)]
pub struct GetQueryParams {
    /// Optional timestamp parameter
    /// If not provided, the current timestamp will be used
    pub timestamp: Option<i64>,
}

/// Query parameters for list requests
#[derive(Debug, Deserialize)]
pub struct ListQueryParams {
    /// Optional key prefix to filter results
    pub key_prefix: Option<String>,
    /// Optional limit on number of results (default: 100, max: 1000)
    pub limit:      Option<i32>,
}

/// Request body for set operations
#[derive(Debug, Deserialize)]
pub struct SetRequest {
    pub series: Series,
}

/// Response structure for get operations
#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub series: Series,
}

/// Response structure for list operations
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub series:   Vec<Series>,
    pub has_more: bool,
}

/// Error response structure
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error:   String,
    pub message: String,
}

impl StoreRestService {
    /// Creates a new StoreRestService instance with the given database
    ///
    /// # Arguments
    /// * `db` - Arc-wrapped database instance for thread-safe access
    pub fn new(db: Arc<DB>) -> Self { Self { db } }

    /// Gets the current timestamp in nanoseconds since Unix epoch
    /// Used as default timestamp when not provided in requests
    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    /// Gets all unique keys from the database by performing a full range scan
    /// Returns the latest value for each key
    fn get_all_keys_with_latest_values(
        &self,
        key_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Series>, rsketch_db::Error> {
        // Use a very wide range to get all data
        let start_key = key_prefix.map(|p| p.as_bytes()).unwrap_or(&[]);
        let end_key = if let Some(prefix) = key_prefix {
            // Create end key by incrementing the last byte of the prefix
            let mut end = prefix.as_bytes().to_vec();
            if let Some(last_byte) = end.last_mut() {
                if *last_byte == 255 {
                    // If last byte is 255, we can't increment it, so use a longer prefix
                    end.push(0);
                } else {
                    *last_byte += 1;
                }
            }
            end
        } else {
            vec![255; 64] // Use a sufficiently large end key for full range
        };

        // Get all data from the beginning of time to now
        let entries = self
            .db
            .range(start_key, &end_key, 0, Self::current_timestamp())?;

        // Group by key and keep only the latest entry for each key
        use std::collections::HashMap;
        let mut latest_by_key: HashMap<Vec<u8>, (Vec<u8>, u64)> = HashMap::new();

        for (key, value, timestamp) in entries {
            match latest_by_key.entry(key.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((value, timestamp));
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let (_, existing_timestamp) = entry.get();
                    if timestamp > *existing_timestamp {
                        entry.insert((value, timestamp));
                    }
                }
            }
        }

        // Convert to Series and sort by key
        let mut series_list: Vec<Series> = latest_by_key
            .into_iter()
            .filter_map(|(key_bytes, (value_bytes, timestamp))| {
                // Convert bytes to strings
                let key = String::from_utf8(key_bytes).ok()?;
                let value = String::from_utf8(value_bytes).ok()?;
                Some(Series {
                    key,
                    value,
                    timestamp: timestamp as i64,
                })
            })
            .collect();

        // Sort by key for consistent ordering
        series_list.sort_by(|a, b| a.key.cmp(&b.key));

        // Apply limit
        if series_list.len() > limit {
            series_list.truncate(limit);
        }

        Ok(series_list)
    }

    /// Converts database errors to HTTP error responses
    fn db_error_to_response(error: rsketch_db::Error) -> impl IntoResponse {
        error!("Database error: {:?}", error);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error:   "DatabaseError".to_string(),
                message: format!("Database operation failed: {error}"),
            }),
        )
    }

    /// Creates an error response with the given status code and message
    fn error_response(status: StatusCode, error_type: &str, message: &str) -> impl IntoResponse {
        (
            status,
            Json(ErrorResponse {
                error:   error_type.to_string(),
                message: message.to_string(),
            }),
        )
    }
}

/// HTTP GET handler for retrieving values by key
///
/// GET /api/v1/store/{key}?timestamp=<optional_timestamp>
///
/// If no timestamp is provided, returns the latest value.
/// If a timestamp is provided, returns the most recent value
/// that is not newer than the specified timestamp.
async fn get_handler(
    Path(key): Path<String>,
    Query(params): Query<GetQueryParams>,
    State(service): State<StoreRestService>,
) -> impl IntoResponse {
    debug!("Received get request for key: {}", key);

    // Use provided timestamp or current time if not specified
    let timestamp = params
        .timestamp
        .map(|ts| ts as u64)
        .unwrap_or_else(StoreRestService::current_timestamp);

    // Get value from database
    match service.db.get(key.as_bytes(), timestamp) {
        Ok(Some(value)) => {
            debug!("Found value for key '{}' at timestamp {}", key, timestamp);

            // Convert bytes back to string for response
            match String::from_utf8(value) {
                Ok(value_str) => {
                    let series = Series {
                        key,
                        value: value_str,
                        timestamp: timestamp as i64,
                    };

                    (StatusCode::OK, Json(GetResponse { series })).into_response()
                }
                Err(e) => {
                    warn!("Failed to convert stored value to string: {}", e);
                    StoreRestService::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "EncodingError",
                        "Stored value is not valid UTF-8",
                    )
                    .into_response()
                }
            }
        }
        Ok(None) => {
            debug!(
                "No value found for key '{}' at timestamp {}",
                key, timestamp
            );
            StoreRestService::error_response(
                StatusCode::NOT_FOUND,
                "KeyNotFound",
                &format!("Key '{key}' not found"),
            )
            .into_response()
        }
        Err(e) => StoreRestService::db_error_to_response(e).into_response(),
    }
}

/// HTTP POST handler for storing key-value pairs
///
/// POST /api/v1/store
/// Content-Type: application/json
///
/// Request body:
/// {
///   "series": {
///     "key": "sensor_01",
///     "value": "23.5",
///     "timestamp": 1641024000000
///   }
/// }
async fn set_handler(
    State(service): State<StoreRestService>,
    Json(request): Json<SetRequest>,
) -> impl IntoResponse {
    let series = request.series;

    debug!(
        "Received set request for key: '{}', value: '{}', timestamp: {}",
        series.key, series.value, series.timestamp
    );

    // Validate timestamp (must be non-negative)
    if series.timestamp < 0 {
        return StoreRestService::error_response(
            StatusCode::BAD_REQUEST,
            "InvalidTimestamp",
            "Timestamp must be non-negative",
        )
        .into_response();
    }

    // Store the key-value pair in the database
    match service.db.put(
        series.key.as_bytes(),
        series.value.as_bytes(),
        series.timestamp as u64,
    ) {
        Ok(()) => {
            debug!(
                "Successfully stored key '{}' with value '{}' at timestamp {}",
                series.key, series.value, series.timestamp
            );
            (StatusCode::OK, "OK").into_response()
        }
        Err(e) => StoreRestService::db_error_to_response(e).into_response(),
    }
}

/// HTTP GET handler for listing all stored keys with their latest values
///
/// GET /api/v1/store?key_prefix=<optional_prefix>&limit=<optional_limit>
///
/// Query parameters:
/// - key_prefix: Optional prefix to filter keys
/// - limit: Optional limit on number of results (default: 100, max: 1000)
///
/// Returns a JSON object with an array of series and a has_more flag
async fn list_handler(
    Query(params): Query<ListQueryParams>,
    State(service): State<StoreRestService>,
) -> impl IntoResponse {
    debug!(
        "Received list request with prefix: {:?}, limit: {:?}",
        params.key_prefix, params.limit
    );

    // Validate and set limit (default: 100, max: 1000)
    let limit = params.limit.unwrap_or(100) as usize;
    if limit > 1000 {
        return StoreRestService::error_response(
            StatusCode::BAD_REQUEST,
            "InvalidLimit",
            "Limit cannot exceed 1000",
        )
        .into_response();
    }
    if limit == 0 {
        return StoreRestService::error_response(
            StatusCode::BAD_REQUEST,
            "InvalidLimit",
            "Limit must be greater than 0",
        )
        .into_response();
    }

    // Get key prefix if provided
    let key_prefix = params.key_prefix.as_deref();

    // Retrieve all keys with their latest values
    match service.get_all_keys_with_latest_values(key_prefix, limit + 1) {
        Ok(mut series_list) => {
            // Check if there are more results than the limit
            let has_more = series_list.len() > limit;
            if has_more {
                series_list.pop(); // Remove the extra item we fetched
            }

            debug!(
                "Returning {} series, has_more: {}",
                series_list.len(),
                has_more
            );

            (
                StatusCode::OK,
                Json(ListResponse {
                    series: series_list,
                    has_more,
                }),
            )
                .into_response()
        }
        Err(e) => StoreRestService::db_error_to_response(e).into_response(),
    }
}

/// Creates the REST API routes for the Store service
///
/// This function returns a router configuration function that can be
/// used with the HTTP server to register the store endpoints.
///
/// # Arguments
/// * `db` - Arc-wrapped database instance for thread-safe access
///
/// # Returns
/// A function that takes a Router and returns a Router with store routes added
pub fn create_store_routes(db: Arc<DB>) -> impl Fn(Router) -> Router + Send + Sync + 'static {
    move |router: Router| -> Router {
        let service = StoreRestService::new(db.clone());

        // Create a sub-router with state, then merge it with the main router
        let store_router = Router::new()
            .route("/api/v1/store/{key}", get(get_handler))
            .route("/api/v1/store", post(set_handler))
            .route("/api/v1/store", get(list_handler))
            .with_state(service);

        router.merge(store_router)
    }
}
