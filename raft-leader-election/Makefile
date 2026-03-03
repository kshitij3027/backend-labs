.PHONY: build test proto clean run stop logs e2e

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

# Start the 5-node cluster
run:
	docker compose up --build -d

# Stop the cluster
stop:
	docker compose down

# View cluster logs
logs:
	docker compose logs -f

# Run E2E tests against the cluster
e2e:
	docker compose up --build -d
	sleep 5
	docker compose run --rm --no-deps -e PYTHONPATH=/app node-1 python scripts/verify_cluster.py || (docker compose down && exit 1)
	docker compose down

# Compile proto files locally (requires grpcio-tools)
proto:
	chmod +x compile_proto.sh && ./compile_proto.sh

# Clean up Docker images and containers
clean:
	docker compose down 2>/dev/null || true
	docker rmi -f $(TEST_IMAGE) $(NODE_IMAGE) 2>/dev/null || true
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
