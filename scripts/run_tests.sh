#!/bin/bash

# Script to run integration tests using Docker Compose
# This will start the server, wait for it to be healthy, then run tests

set -e

echo "🚀 Starting timeseries-engine with Docker Compose..."
echo "=================================================="

# Start the server in the background
cd docker
docker-compose up -d timeseries-engine

echo "⏳ Waiting for server to be healthy..."

# Wait for healthcheck to pass
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker-compose ps timeseries-engine | grep -q "healthy"; then
        echo "✅ Server is healthy!"
        break
    fi
    
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts - waiting..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ Server failed to become healthy within timeout"
    echo "📋 Server logs:"
    docker-compose logs timeseries-engine
    exit 1
fi

echo ""
echo "🧪 Running integration tests..."
echo "==============================="

# Run the test container
if docker-compose --profile test run --rm test-runner; then
    echo ""
    echo "🎉 All tests passed!"
    test_result=0
else
    echo ""
    echo "❌ Some tests failed!"
    test_result=1
fi

echo ""
echo "🛑 Stopping services..."
docker-compose down

echo "📊 Test Results: $([ $test_result -eq 0 ] && echo "SUCCESS" || echo "FAILED")"
exit $test_result 