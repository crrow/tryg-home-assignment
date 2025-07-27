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
