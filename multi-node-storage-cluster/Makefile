.PHONY: build test run stop e2e clean logs loadtest

PROJECT := multi-node-storage-cluster

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	docker compose up --build -d node1 node2 node3

stop:
	docker compose down -v

e2e:
	docker compose up --build -d node1
	sleep 3
	@echo "=== Health check ==="
	curl -sf http://localhost:5001/health | python3 -m json.tool
	@echo "\n=== Write test ==="
	curl -sf -X POST http://localhost:5001/write \
		-H "Content-Type: application/json" \
		-d '{"message":"hello","level":"info"}' | python3 -m json.tool
	@echo "\n=== List files ==="
	curl -sf http://localhost:5001/files | python3 -m json.tool
	@echo "\n=== Stats check ==="
	curl -sf http://localhost:5001/stats | python3 -m json.tool
	docker compose down -v
	@echo "\n=== E2E PASSED ==="

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

logs:
	docker compose logs -f

loadtest:
	@echo "Load tests - to be implemented in Commit 9"
