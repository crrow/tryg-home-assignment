# Scripts

This directory contains utility scripts for testing and managing the timeseries-engine.

## Scripts

### `test_server.sh`
**Purpose**: Comprehensive integration test suite for the timeseries-engine REST API.

**Usage**:
```bash
# Test against local server (default: localhost:3000)
./scripts/test_server.sh

# Test against custom host:port
./scripts/test_server.sh myserver.com:8080
```

**What it tests**:
- ✅ Health endpoint
- ✅ Store time series data (multiple sensors)
- ✅ Retrieve data by timestamp
- ✅ Time series behavior (latest value before timestamp)
- ✅ Error handling (404, 400)
- ✅ Performance (bulk operations)

**Requirements**: `curl`, `bash`

### `run_tests.sh`
**Purpose**: Automated Docker Compose test runner that starts the server and runs the full test suite.

**Usage**:
```bash
# From project root
./scripts/run_tests.sh
```

**What it does**:
1. Starts timeseries-engine with Docker Compose
2. Waits for server health check to pass
3. Runs integration tests in a container
4. Reports results and cleans up

**Requirements**: `docker`, `docker-compose`

## Development Workflow

### Quick Local Testing
```bash
# Start server locally
cargo run -- server --host 127.0.0.1 --port 50051 --http-port 3000 --db-path ./data

# In another terminal, run tests
./scripts/test_server.sh
```

### Full Docker Testing
```bash
# Complete test cycle with Docker
./scripts/run_tests.sh
```

### Manual Docker Testing
```bash
cd docker

# Start server
docker-compose up -d timeseries-engine

# Wait for health check, then run tests
docker-compose --profile test run --rm test-runner

# Cleanup
docker-compose down
```

## Output Examples

**Successful test run**:
```
🧪 Testing timeseries-engine server at http://localhost:3000
================================================
1. Basic Health Check
--------------------
Testing Health endpoint... ✓ PASS (HTTP 200)

2. Store Data Tests
------------------
Testing Store temperature data... ✓ PASS (HTTP 200)
[...]

📊 Test Results
===============
Tests run: 10
Tests passed: 10
Tests failed: 0
🎉 All tests passed!
```

**Failed test**:
```
Testing Health endpoint... ✗ FAIL (Expected HTTP 200, got HTTP 502)
   Response: 
```

## Adding New Tests

To add new tests to `test_server.sh`:

1. Add test cases in the appropriate section
2. Use the helper functions:
   - `run_test "description" "expected_status" "url"`
   - `run_test_with_json "description" "expected_status" "json_data" "url"`
3. Update the test count logic if needed

Example:
```bash
# Add to the appropriate section
run_test "Get sensor list" "200" "$BASE_URL/api/v1/sensors"
``` 