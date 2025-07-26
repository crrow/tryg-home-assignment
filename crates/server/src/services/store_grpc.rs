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

use rsketch_api::pb::store::v1::{GetRequest, GetResponse, Series, SetRequest};
use rsketch_db::DB;
use tonic::{Request, Response, Status, service::RoutesBuilder};
use tracing::{debug, error, warn};

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

    /// Converts database errors to gRPC Status errors
    fn db_error_to_status(error: rsketch_db::Error) -> Status {
        error!("Database error: {:?}", error);
        Status::internal(format!("Database operation failed: {error}"))
    }
}

#[async_trait::async_trait]
impl rsketch_api::pb::store::v1::store_server::Store for StoreSvc {
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
}

#[async_trait::async_trait]
impl GrpcServiceHandler for StoreSvc {
    fn service_name(&self) -> &'static str { "Store" }

    fn file_descriptor_set(&self) -> &'static [u8] { rsketch_api::pb::GRPC_DESC }

    fn register_service(self: &Arc<Self>, builder: &mut RoutesBuilder) {
        use tonic::service::LayerExt as _;
        let svc = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::new())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                rsketch_api::pb::store::v1::store_server::StoreServer::from_arc(self.clone()),
            );
        builder.add_service(svc);
    }
}
