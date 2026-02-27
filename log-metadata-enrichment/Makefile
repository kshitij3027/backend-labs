.PHONY: build test run verify e2e clean

IMAGE_NAME = log-enrichment
TEST_IMAGE = log-enrichment-test
CONTAINER_NAME = log-enrichment-app

build:
	docker build -t $(IMAGE_NAME) .

test:
	docker build -f Dockerfile.test -t $(TEST_IMAGE) .
	docker run --rm $(TEST_IMAGE)

run: build
	docker run --rm -p 8080:8080 --name $(CONTAINER_NAME) $(IMAGE_NAME)

verify:
	docker build -f Dockerfile.test -t $(TEST_IMAGE) .
	docker run --rm $(TEST_IMAGE) bash /app/verify.sh

e2e: build
	@echo "=== Starting E2E tests ==="
	@# Start server in background
	docker run -d --rm -p 8080:8080 --name $(CONTAINER_NAME) $(IMAGE_NAME)
	@# Wait for health check
	@echo "Waiting for server to start..."
	@for i in $$(seq 1 30); do \
		if docker exec $(CONTAINER_NAME) python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" 2>/dev/null; then \
			echo "Server is ready!"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "Server failed to start"; \
			docker stop $(CONTAINER_NAME) 2>/dev/null || true; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@# Test health endpoint
	@echo "--- Testing /health ---"
	curl -sf http://localhost:8080/health | python3 -m json.tool
	@echo ""
	@# Test enrich endpoint
	@echo "--- Testing POST /api/enrich ---"
	curl -sf -X POST http://localhost:8080/api/enrich \
		-H "Content-Type: application/json" \
		-d '{"log_message": "ERROR: Database connection failed", "source": "e2e-test"}' | python3 -m json.tool
	@echo ""
	@# Test stats endpoint
	@echo "--- Testing GET /api/stats ---"
	curl -sf http://localhost:8080/api/stats | python3 -m json.tool
	@echo ""
	@# Test sample-logs endpoint
	@echo "--- Testing GET /api/sample-logs ---"
	curl -sf http://localhost:8080/api/sample-logs | python3 -m json.tool
	@echo ""
	@# Test enrichment field count (8+ fields required)
	@echo "--- Verifying 8+ enrichment fields ---"
	@FIELD_COUNT=$$(curl -sf -X POST http://localhost:8080/api/enrich \
		-H "Content-Type: application/json" \
		-d '{"log_message": "INFO: test message"}' | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"); \
	echo "Field count: $$FIELD_COUNT"; \
	if [ $$FIELD_COUNT -lt 8 ]; then \
		echo "FAIL: Expected 8+ fields, got $$FIELD_COUNT"; \
		docker stop $(CONTAINER_NAME) 2>/dev/null || true; \
		exit 1; \
	fi; \
	echo "PASS: $$FIELD_COUNT fields"
	@echo ""
	@# Throughput test (100 logs)
	@echo "--- Throughput test (100 logs) ---"
	@START=$$(python3 -c "import time; print(time.time())"); \
	for i in $$(seq 1 100); do \
		curl -sf -X POST http://localhost:8080/api/enrich \
			-H "Content-Type: application/json" \
			-d "{\"log_message\": \"INFO: Throughput test message $$i\"}" > /dev/null; \
	done; \
	END=$$(python3 -c "import time; print(time.time())"); \
	ELAPSED=$$(python3 -c "print(round($$END - $$START, 2))"); \
	THROUGHPUT=$$(python3 -c "print(round(100 / ($$END - $$START), 1))"); \
	echo "100 logs in $$ELAPSED seconds ($$THROUGHPUT logs/sec)"; \
	python3 -c "assert 100 / ($$END - $$START) > 10, f'Throughput too low: {100/($$END-$$START):.1f} logs/sec'" && echo "PASS: Throughput > 10 logs/sec" || (echo "FAIL: Throughput < 10 logs/sec"; docker stop $(CONTAINER_NAME) 2>/dev/null || true; exit 1)
	@echo ""
	@# Verify stats after throughput test
	@echo "--- Final stats ---"
	curl -sf http://localhost:8080/api/stats | python3 -m json.tool
	@echo ""
	@echo "=== All E2E tests passed ==="
	@# Cleanup
	docker stop $(CONTAINER_NAME) 2>/dev/null || true

clean:
	docker stop $(CONTAINER_NAME) 2>/dev/null || true
	docker rmi $(IMAGE_NAME) $(TEST_IMAGE) 2>/dev/null || true
