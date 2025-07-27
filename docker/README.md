# Docker Setup for Timeseries Engine

This directory contains Docker configuration for running the timeseries-engine server and integration tests.

## Quick Start

### Option 1: Run Server Only
```bash
# Start the server
cd docker
docker-compose up -d

# Check server status
docker-compose ps
docker-compose logs -f timeseries-engine

# Test manually (server will be available at http://localhost:3000)
curl http://localhost:3000/health

# Stop the server
docker-compose down
```

### Option 2: Run Server + Automated Tests

**Simple approach:**
```bash
# Run complete test suite (from project root)
chmod +x scripts/run_tests.sh
./scripts/run_tests.sh
```

**Manual approach:**
```bash
cd docker

# Start server and wait for health check
docker-compose up -d timeseries-engine

# Run tests (will wait for server to be healthy)
docker-compose --profile test run --rm test-runner

# Stop everything
docker-compose down
```

## Files

- `docker-compose.yml` - Main compose configuration
- `Dockerfile` - Production server image
- `Dockerfile.test` - Lightweight test runner image  
- `README.md` - This file

## Environment Variables

### For Server (`timeseries-engine`)
- `RUST_LOG=info` - Logging level
- `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` - Proxy settings for build

### For Tests (`test-runner`)
- `SERVER_URL=http://timeseries-engine:3000` - Target server URL

## Networking

Services communicate over the default Docker Compose network:
- Server binds to `0.0.0.0:3000` and `0.0.0.0:50051` inside container
- Tests connect to `timeseries-engine:3000` (service name resolution)
- Host can access server at `localhost:3000` and `localhost:50051`

## Troubleshooting

**Server won't start:**
```bash
docker-compose logs timeseries-engine
```

**Tests failing:**
```bash
# Run tests with detailed output
docker-compose --profile test run --rm test-runner

# Check server health manually
docker-compose exec timeseries-engine curl http://localhost:3000/health
```

**Build issues:**
```bash
# Rebuild images
docker-compose build --no-cache
```

**Data persistence:**
```bash
# View data volume
docker volume inspect docker_timeseries_data

# Remove data (clean slate)
docker-compose down -v
``` 