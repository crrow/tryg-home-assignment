#!/bin/bash

# Test script for the running timeseries-engine server
# Usage: ./test_server.sh [host:port]
# Default: localhost:3000

set -e

HOST_PORT=${1:-localhost:3000}
BASE_URL="http://$HOST_PORT"

echo "🧪 Testing timeseries-engine server at $BASE_URL"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_RUN=0
TESTS_PASSED=0

run_test() {
    local test_name="$1"
    local expected_status="$2"
    shift 2
    
    echo -n "Testing $test_name... "
    TESTS_RUN=$((TESTS_RUN + 1))
    
    # Run the curl command and capture both status and response
    response=$(curl -s -w "HTTPSTATUS:%{http_code}" "$@" 2>/dev/null || echo "HTTPSTATUS:000")
    
    # Extract status code and body
    status=$(echo "$response" | grep -o "HTTPSTATUS:[0-9]*" | cut -d: -f2)
    body=$(echo "$response" | sed 's/HTTPSTATUS:[0-9]*$//')
    
    if [ "$status" = "$expected_status" ]; then
        echo -e "${GREEN}✓ PASS${NC} (HTTP $status)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        if [ -n "$body" ] && [ "$body" != "OK" ]; then
            echo "   Response: $body"
        fi
    else
        echo -e "${RED}✗ FAIL${NC} (Expected HTTP $expected_status, got HTTP $status)"
        if [ -n "$body" ]; then
            echo "   Response: $body"
        fi
    fi
}

run_test_with_json() {
    local test_name="$1"
    local expected_status="$2"
    local json_data="$3"
    shift 3
    
    echo -n "Testing $test_name... "
    TESTS_RUN=$((TESTS_RUN + 1))
    
    # Run the curl command with JSON data
    response=$(curl -s -w "HTTPSTATUS:%{http_code}" \
        -H "Content-Type: application/json" \
        -d "$json_data" \
        "$@" 2>/dev/null || echo "HTTPSTATUS:000")
    
    # Extract status code and body
    status=$(echo "$response" | grep -o "HTTPSTATUS:[0-9]*" | cut -d: -f2)
    body=$(echo "$response" | sed 's/HTTPSTATUS:[0-9]*$//')
    
    if [ "$status" = "$expected_status" ]; then
        echo -e "${GREEN}✓ PASS${NC} (HTTP $status)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        if [ -n "$body" ] && [ "$body" != "OK" ]; then
            echo "   Response: $body"
        fi
    else
        echo -e "${RED}✗ FAIL${NC} (Expected HTTP $expected_status, got HTTP $status)"
        if [ -n "$body" ]; then
            echo "   Response: $body"
        fi
    fi
}

echo "1. Basic Health Check"
echo "--------------------"
run_test "Health endpoint" "200" "$BASE_URL/health"
echo

echo "2. Store Data Tests"
echo "------------------"

# Test storing data
STORE_DATA='{
    "series": {
        "key": "temperature_sensor_01",
        "value": "23.5",
        "timestamp": 1641024000
    }
}'

run_test_with_json "Store temperature data" "200" "$STORE_DATA" "$BASE_URL/api/v1/store"

# Test storing more data points
STORE_DATA2='{
    "series": {
        "key": "temperature_sensor_01", 
        "value": "24.1",
        "timestamp": 1641024060
    }
}'

run_test_with_json "Store second temperature reading" "200" "$STORE_DATA2" "$BASE_URL/api/v1/store"

# Test storing different sensor
STORE_DATA3='{
    "series": {
        "key": "humidity_sensor_01",
        "value": "65.2", 
        "timestamp": 1641024000
    }
}'

run_test_with_json "Store humidity data" "200" "$STORE_DATA3" "$BASE_URL/api/v1/store"

echo

echo "3. Retrieve Data Tests"
echo "---------------------"

# Test retrieving data
run_test "Get temperature at exact timestamp" "200" "$BASE_URL/api/v1/store/temperature_sensor_01?timestamp=1641024000"

run_test "Get temperature at later timestamp" "200" "$BASE_URL/api/v1/store/temperature_sensor_01?timestamp=1641024060"

run_test "Get humidity data" "200" "$BASE_URL/api/v1/store/humidity_sensor_01?timestamp=1641024000"

# Test time series behavior - should get latest value before timestamp
run_test "Get latest temperature before timestamp" "200" "$BASE_URL/api/v1/store/temperature_sensor_01?timestamp=1641024100"

echo

echo "4. Edge Cases"
echo "-------------"

# Test non-existent key
run_test "Get non-existent key" "404" "$BASE_URL/api/v1/store/nonexistent_sensor?timestamp=1641024000"

# Test invalid JSON
run_test_with_json "Invalid JSON data" "400" '{"invalid": json}' "$BASE_URL/api/v1/store"

echo

echo "5. Performance Test"
echo "------------------"

echo -n "Bulk insert test (10 data points)... "
START_TIME=$(date +%s%N)

for i in {1..10}; do
    timestamp=$((1641024000 + i * 60))
    value="2$i.$(($i % 10))"
    curl -s -X POST "$BASE_URL/api/v1/store" \
        -H "Content-Type: application/json" \
        -d "{\"series\": {\"key\": \"perf_test_sensor\", \"value\": \"$value\", \"timestamp\": $timestamp}}" \
        > /dev/null
done

END_TIME=$(date +%s%N)
DURATION=$(( (END_TIME - START_TIME) / 1000000 )) # Convert to milliseconds

echo -e "${GREEN}✓ COMPLETE${NC} (${DURATION}ms for 10 operations)"

echo

echo "📊 Test Results"
echo "==============="
echo "Tests run: $TESTS_RUN"
echo "Tests passed: $TESTS_PASSED"
echo "Tests failed: $((TESTS_RUN - TESTS_PASSED))"

if [ $TESTS_PASSED -eq $TESTS_RUN ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed.${NC}"
    exit 1
fi 