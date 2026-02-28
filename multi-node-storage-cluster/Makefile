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
	docker compose down -v 2>/dev/null || true
	docker compose up --build -d node1 node2 node3 dashboard
	sleep 5
	@echo "=== Dashboard health ==="
	curl -sf http://localhost:8080/api/health | python3 -m json.tool
	@echo "\n=== Write via dashboard API ==="
	curl -sf -X POST http://localhost:8080/api/write \
		-H "Content-Type: application/json" \
		-d '{"message":"client test 1","level":"info"}' | python3 -m json.tool
	curl -sf -X POST http://localhost:8080/api/write \
		-H "Content-Type: application/json" \
		-d '{"message":"client test 2","level":"debug"}' | python3 -m json.tool
	curl -sf -X POST http://localhost:8080/api/write \
		-H "Content-Type: application/json" \
		-d '{"message":"client test 3","level":"warn"}' | python3 -m json.tool
	sleep 3
	@echo "\n=== Files via dashboard ==="
	curl -sf http://localhost:8080/api/files | python3 -m json.tool
	@echo "\n=== Cluster status ==="
	curl -sf http://localhost:8080/api/cluster | python3 -m json.tool
	docker compose down -v
	@echo "\n=== E2E PASSED ==="

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

logs:
	docker compose logs -f

loadtest:
	@echo "Load tests - to be implemented in Commit 9"
