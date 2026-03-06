.PHONY: build test run stop e2e clean logs

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	docker compose up --build -d coordinator

stop:
	docker compose down

e2e: build
	@echo "=== Starting E2E Tests ==="
	docker compose up -d coordinator
	@sleep 5
	@echo "--- Health Checks ---"
	curl -sf http://localhost:8000/health | python3 -m json.tool
	@echo "--- Cluster Status ---"
	curl -sf http://localhost:8000/api/cluster/status | python3 -m json.tool
	@echo "--- Write 3 Keys ---"
	curl -sf -X POST http://localhost:8000/api/logs -H 'Content-Type: application/json' -d '{"key":"k1","value":"v1"}' | python3 -m json.tool
	curl -sf -X POST http://localhost:8000/api/logs -H 'Content-Type: application/json' -d '{"key":"k2","value":"v2"}' | python3 -m json.tool
	curl -sf -X POST http://localhost:8000/api/logs -H 'Content-Type: application/json' -d '{"key":"k3","value":"v3"}' | python3 -m json.tool
	@echo "--- Read Back ---"
	curl -sf http://localhost:8000/api/logs/k1 | python3 -m json.tool
	curl -sf http://localhost:8000/api/logs/k2 | python3 -m json.tool
	curl -sf http://localhost:8000/api/logs/k3 | python3 -m json.tool
	@echo "--- List Keys ---"
	curl -sf http://localhost:8000/api/logs | python3 -m json.tool
	@echo "--- Switch to STRONG ---"
	curl -sf -X POST http://localhost:8000/api/cluster/config -H 'Content-Type: application/json' -d '{"level":"strong"}' | python3 -m json.tool
	@echo "--- Fail Node-3 ---"
	curl -sf -X POST http://localhost:8000/api/nodes/node-3/fail | python3 -m json.tool
	@echo "--- Write in Degraded STRONG Mode (expect failure) ---"
	curl -s -X POST http://localhost:8000/api/logs -H 'Content-Type: application/json' -d '{"key":"k4","value":"v4"}' | python3 -m json.tool || echo "Expected: write fails in STRONG with node down"
	@echo "--- Switch to BALANCED ---"
	curl -sf -X POST http://localhost:8000/api/cluster/config -H 'Content-Type: application/json' -d '{"level":"balanced"}' | python3 -m json.tool
	@echo "--- Write in BALANCED with 1 node down (should succeed) ---"
	curl -sf -X POST http://localhost:8000/api/logs -H 'Content-Type: application/json' -d '{"key":"k4","value":"v4"}' | python3 -m json.tool
	@echo "--- Recover Node-3 ---"
	curl -sf -X POST http://localhost:8000/api/nodes/node-3/recover | python3 -m json.tool
	@echo "--- Inspect Node-3 Data (should have hinted handoff data) ---"
	curl -sf http://localhost:8000/api/nodes/node-3/data | python3 -m json.tool
	@echo "--- Metrics ---"
	curl -sf http://localhost:8000/metrics | python3 -m json.tool
	@echo "=== E2E Tests Complete ==="
	docker compose down

clean:
	docker compose down -v --rmi local
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

logs:
	docker compose logs -f
