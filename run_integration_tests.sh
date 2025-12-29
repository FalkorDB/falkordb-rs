#!/bin/bash

# Script to run integration tests with FalkorDB in Docker
# This makes it easy for developers to test locally

set -e

echo "Starting FalkorDB container..."

# Check if container is already running
if docker ps | grep -q falkordb-test; then
    echo "FalkorDB test container is already running"
else
    # Remove existing container if it exists
    docker rm -f falkordb-test 2>/dev/null || true
    
    # Start FalkorDB container
    docker run -d --name falkordb-test \
        -p 6379:6379 \
        falkordb/falkordb:latest
    
    echo "Waiting for FalkorDB to be ready..."
    for i in {1..30}; do
        if docker exec falkordb-test redis-cli ping > /dev/null 2>&1; then
            echo "FalkorDB is ready!"
            break
        fi
        echo "Attempt $i: FalkorDB not ready yet..."
        sleep 2
    done
fi

echo ""
echo "Running integration tests..."
export FALKORDB_HOST=127.0.0.1
export FALKORDB_PORT=6379

# Run the tests
cargo test --test integration_tests -- --test-threads=1

echo ""
echo "Tests completed!"
echo ""
echo "To stop the FalkorDB container, run:"
echo "  docker stop falkordb-test && docker rm falkordb-test"
