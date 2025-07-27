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

use tonic::{Request, Response, Status, service::RoutesBuilder};
use tracing::{debug, error, warn};
use tryg_api::pb::store::v1::{
    GetRequest, GetResponse, ListRequest, ListResponse, Series, SetRequest,
};
use tryg_db::DB;

use crate::grpc::GrpcServiceHandler;

/// gRPC Store service implementation
///
/// This service exposes the rsketch-db functionality over gRPC, providing
/// get and set operations for time series data storage and retrieval.
#[derive(Debug)]
pub struct StoreSvc {
    /// Database instance for data storage and retrieval
    db: Arc<DB>,
}

impl StoreSvc {
    /// Creates a new StoreSvc instance with the given database
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
    ) -> Result<Vec<Series>, tryg_db::Error> {
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

    /// Converts database errors to gRPC Status errors
    fn db_error_to_status(error: tryg_db::Error) -> Status {
        error!("Database error: {:?}", error);
        Status::internal(format!("Database operation failed: {error}"))
    }
}

#[async_trait::async_trait]
impl tryg_api::pb::store::v1::store_server::Store for StoreSvc {
    /// Retrieves a value by key, optionally at a specific timestamp
    ///
    /// If no timestamp is provided, returns the latest value.
    /// If a timestamp is provided, returns the most recent value
    /// that is not newer than the specified timestamp.
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        debug!("Received get request for key: {}", req.key);

        // Use provided timestamp or current time if not specified
        let timestamp = req
            .timestamp
            .unwrap_or_else(|| Self::current_timestamp() as i64) as u64;

        // Get value from database
        match self.db.get(req.key.as_bytes(), timestamp) {
            Ok(Some(value)) => {
                debug!(
                    "Found value for key '{}' at timestamp {}",
                    req.key, timestamp
                );

                // Convert bytes back to string for response
                let value_str = String::from_utf8(value).map_err(|e| {
                    warn!("Failed to convert stored value to string: {}", e);
                    Status::internal("Stored value is not valid UTF-8")
                })?;

                let series = Series {
                    key:       req.key,
                    value:     value_str,
                    timestamp: timestamp as i64,
                };

                Ok(Response::new(GetResponse {
                    series: Some(series),
                }))
            }
            Ok(None) => {
                debug!(
                    "No value found for key '{}' at timestamp {}",
                    req.key, timestamp
                );
                Err(Status::not_found(format!("Key '{}' not found", req.key)))
            }
            Err(e) => Err(Self::db_error_to_status(e)),
        }
    }

    /// Stores a key-value pair with a timestamp
    ///
    /// The timestamp is taken from the Series message and represents
    /// when this data point was recorded.
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();

        let series = req
            .series
            .ok_or_else(|| Status::invalid_argument("Series data is required"))?;

        debug!(
            "Received set request for key: '{}', value: '{}', timestamp: {}",
            series.key, series.value, series.timestamp
        );

        // Validate timestamp (must be positive)
        if series.timestamp < 0 {
            return Err(Status::invalid_argument("Timestamp must be non-negative"));
        }

        // Store the key-value pair in the database
        match self.db.put(
            series.key.as_bytes(),
            series.value.as_bytes(),
            series.timestamp as u64,
        ) {
            Ok(()) => {
                debug!(
                    "Successfully stored key '{}' with value '{}' at timestamp {}",
                    series.key, series.value, series.timestamp
                );
                Ok(Response::new(()))
            }
            Err(e) => Err(Self::db_error_to_status(e)),
        }
    }

    /// Lists all stored keys with their latest values
    ///
    /// Optionally filters by key prefix and applies a limit to the number of
    /// results. Returns the latest value for each unique key.
    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Received list request with prefix: {:?}, limit: {:?}",
            req.key_prefix, req.limit
        );

        // Validate and set limit (default: 100, max: 1000)
        let limit = req.limit.unwrap_or(100) as usize;
        if limit > 1000 {
            return Err(Status::invalid_argument("Limit cannot exceed 1000"));
        }
        if limit == 0 {
            return Err(Status::invalid_argument("Limit must be greater than 0"));
        }

        // Get key prefix if provided
        let key_prefix = req.key_prefix.as_deref();

        // Retrieve all keys with their latest values
        match self.get_all_keys_with_latest_values(key_prefix, limit + 1) {
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

                Ok(Response::new(ListResponse {
                    series: series_list,
                    has_more,
                }))
            }
            Err(e) => Err(Self::db_error_to_status(e)),
        }
    }
}

#[async_trait::async_trait]
impl GrpcServiceHandler for StoreSvc {
    fn service_name(&self) -> &'static str { "Store" }

    fn file_descriptor_set(&self) -> &'static [u8] { tryg_api::pb::GRPC_DESC }

    fn register_service(self: &Arc<Self>, builder: &mut RoutesBuilder) {
        use tonic::service::LayerExt as _;
        let svc = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::new())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                tryg_api::pb::store::v1::store_server::StoreServer::from_arc(self.clone()),
            );
        builder.add_service(svc);
    }
}
