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

//! Basic usage example for RSketch client library
//!
//! This example demonstrates how to use both gRPC and REST clients
//! to interact with the RSketch timeseries database.
//!
//! This example starts its own server instance and then tests both clients.
//! To run this example: `cargo run --example basic_usage`

use std::time::{Duration, SystemTime};

use rsketch_client::{GrpcClient, RestClient, Series};
use tokio::time::sleep;
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting RSketch client examples with embedded server");

    // Start the server in the background
    info!("Starting embedded server...");
    let server_handle = tokio::spawn(async {
        // Create server configuration
        let config = rsketch_server::ServerConfig {
            host:      "127.0.0.1".to_string(),
            grpc_port: 9090,
            http_port: 8080,
            db_path:   "./data/example_db".to_string(),
        };

        // Create and start the server
        let server_builder =
            rsketch_server::ServerBuilder::new(config).expect("Failed to create server builder");

        let (_grpc_handle, _rest_handle) = server_builder
            .start()
            .await
            .expect("Failed to start server");

        info!("Server started successfully, keeping alive...");

        // Keep the server running
        futures::future::pending::<()>().await;
    });

    // Give the server a moment to start up
    info!("Waiting for server to start...");
    sleep(Duration::from_millis(2000)).await; // Increased wait time

    // Test server health first
    info!("Testing server health...");
    let health_response = reqwest::get("http://127.0.0.1:8080/health").await;
    match health_response {
        Ok(resp) => info!("Server health check: {}", resp.status()),
        Err(e) => {
            eprintln!("Server health check failed: {e}");
            return Ok(());
        }
    }

    // Test manual REST request
    info!("Testing manual REST request...");
    let client = reqwest::Client::new();
    let test_payload = serde_json::json!({
        "series": {
            "key": "manual_test",
            "value": "42.0",
            "timestamp": 1641024000000_i64
        }
    });

    let manual_response = client
        .post("http://127.0.0.1:8080/api/v1/store")
        .json(&test_payload)
        .send()
        .await;

    match manual_response {
        Ok(resp) => info!("Manual REST request: {}", resp.status()),
        Err(e) => eprintln!("Manual REST request failed: {e}"),
    }

    // Test manual list request
    info!("Testing manual list request...");
    let list_response = reqwest::get("http://127.0.0.1:8080/api/v1/store/list").await;
    match list_response {
        Ok(resp) => info!("Manual list request: {}", resp.status()),
        Err(e) => eprintln!("Manual list request failed: {e}"),
    }

    // Test data
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos() as i64;

    let test_series = vec![
        Series {
            key: "temperature_sensor_1".to_string(),
            value: "23.5".to_string(),
            timestamp,
        },
        Series {
            key:       "temperature_sensor_2".to_string(),
            value:     "24.1".to_string(),
            timestamp: timestamp + 1000000, // 1ms later
        },
        Series {
            key:       "humidity_sensor_1".to_string(),
            value:     "65.2".to_string(),
            timestamp: timestamp + 2000000, // 2ms later
        },
    ];

    // Test gRPC client
    info!("Testing gRPC client...");
    if let Err(e) = test_grpc_client(&test_series).await {
        eprintln!("gRPC client test failed: {e}");
    }

    // Test REST client
    info!("Testing REST client...");
    if let Err(e) = test_rest_client(&test_series).await {
        eprintln!("REST client test failed: {e}");
        eprintln!("Error details: {e:?}");
    }

    info!("Examples completed successfully!");

    // Abort the server
    server_handle.abort();

    Ok(())
}

async fn test_grpc_client(test_data: &[Series]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Connecting to gRPC server...");
    let mut client = GrpcClient::connect("http://localhost:9090").await?;

    // Store test data
    info!("Storing test data via gRPC...");
    for series in test_data {
        client.set(series.clone()).await?;
        info!("Stored: {} = {}", series.key, series.value);
    }

    // Retrieve data
    info!("Retrieving data via gRPC...");
    for series in test_data {
        match client.get(&series.key, None).await {
            Ok(retrieved) => {
                info!(
                    "Retrieved: {} = {} (timestamp: {})",
                    retrieved.key, retrieved.value, retrieved.timestamp
                );
            }
            Err(e) => {
                eprintln!("Failed to retrieve {}: {}", series.key, e);
            }
        }
    }

    // List all data
    info!("Listing all data via gRPC...");
    let list_result = client.list(None, Some(10)).await?;
    info!("Found {} series total", list_result.series.len());
    for series in &list_result.series[..list_result.series.len().min(3)] {
        info!("  {}: {}", series.key, series.value);
    }
    if list_result.series.len() > 3 {
        info!("  ... and {} more", list_result.series.len() - 3);
    }

    // Test prefix filtering
    info!("Testing prefix filtering via gRPC...");
    let temp_list = client
        .list(Some("temperature".to_string()), Some(5))
        .await?;
    info!("Found {} temperature sensors", temp_list.series.len());

    Ok(())
}

async fn test_rest_client(test_data: &[Series]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Creating REST client...");
    let client = RestClient::new("http://127.0.0.1:8080")?;

    // Store test data
    info!("Storing test data via REST...");
    for series in test_data {
        info!("About to store: {} = {}", series.key, series.value);
        match client.set(series.clone()).await {
            Ok(()) => {
                info!("Stored: {} = {}", series.key, series.value);
            }
            Err(e) => {
                eprintln!("Failed to store {}: {}", series.key, e);
                eprintln!("Error details: {e:?}");
                return Err(e.into());
            }
        }
    }

    // Retrieve data
    info!("Retrieving data via REST...");
    for series in test_data {
        match client.get(&series.key, None).await {
            Ok(retrieved) => {
                info!(
                    "Retrieved: {} = {} (timestamp: {})",
                    retrieved.key, retrieved.value, retrieved.timestamp
                );
            }
            Err(e) => {
                eprintln!("Failed to retrieve {}: {}", series.key, e);
            }
        }
    }

    // List all data
    info!("Listing all data via REST...");
    let list_result = client.list(None, Some(10)).await?;
    info!("Found {} series total", list_result.series.len());
    for series in &list_result.series[..list_result.series.len().min(3)] {
        info!("  {}: {}", series.key, series.value);
    }
    if list_result.series.len() > 3 {
        info!("  ... and {} more", list_result.series.len() - 3);
    }

    // Test prefix filtering
    info!("Testing prefix filtering via REST...");
    let temp_list = client
        .list(Some("temperature".to_string()), Some(5))
        .await?;
    info!("Found {} temperature sensors", temp_list.series.len());

    Ok(())
}
