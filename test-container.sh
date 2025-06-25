#!/bin/bash

set -e

echo "🔨 Building container image..."

# Get current commit SHA
COMMIT_SHA=$(git rev-parse HEAD)
echo "Commit SHA: $COMMIT_SHA"

# Build the image
docker build \
  --build-arg COMMIT_SHA=$COMMIT_SHA \
  -t gitlab-mr-reviewer:test \
  .

echo "✅ Container built successfully"

echo "🚀 Starting container..."

# Run container in background
CONTAINER_ID=$(docker run -d \
  --publish 8080:8000 \
  --env COMMIT_SHA=$COMMIT_SHA \
  --env GITLAB_API_TOKEN=dummy \
  --env WEBHOOK_SECRET=test-secret \
  gitlab-mr-reviewer:test)

echo "Container ID: $CONTAINER_ID"

# Wait for startup
echo "⏳ Waiting for container to start..."
sleep 10

# Test health check
echo "🏥 Testing health check..."
for i in {1..10}; do
  if RESPONSE=$(curl -s -f http://localhost:8080/health 2>/dev/null); then
    echo "✅ Health check successful!"
    echo "Response: $RESPONSE"
    
    # Check if commit SHA is in response
    if echo "$RESPONSE" | grep -q "$COMMIT_SHA"; then
      echo "✅ Commit SHA found in response"
    else
      echo "⚠️  Commit SHA not found in response"
    fi
    break
  else
    echo "⏳ Health check attempt $i failed, retrying..."
    sleep 2
  fi
  
  if [ $i -eq 10 ]; then
    echo "❌ Health check failed after 10 attempts"
    docker logs $CONTAINER_ID
    docker rm -f $CONTAINER_ID
    exit 1
  fi
done

# Cleanup
echo "🧹 Cleaning up..."
docker rm -f $CONTAINER_ID

echo "🎉 Test completed successfully!" 