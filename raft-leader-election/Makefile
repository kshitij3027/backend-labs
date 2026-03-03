.PHONY: build test proto clean

PROJECT_NAME = raft-leader-election
TEST_IMAGE = $(PROJECT_NAME)-test
NODE_IMAGE = $(PROJECT_NAME)-node

# Build the main node image
build:
	docker build -t $(NODE_IMAGE) -f Dockerfile .

# Build and run unit tests in Docker
test:
	docker build -t $(TEST_IMAGE) -f Dockerfile.test .
	docker run --rm $(TEST_IMAGE)

# Compile proto files locally (requires grpcio-tools)
proto:
	chmod +x compile_proto.sh && ./compile_proto.sh

# Clean up Docker images
clean:
	docker rmi -f $(TEST_IMAGE) $(NODE_IMAGE) 2>/dev/null || true
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
