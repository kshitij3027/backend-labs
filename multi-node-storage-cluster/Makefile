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
	docker compose up --build -d node1 node2 node3
	sleep 5
	@echo "=== All nodes healthy ==="
	curl -sf http://localhost:5001/cluster/status | python3 -m json.tool
	@echo "\n=== Write to node1 ==="
	curl -sf -X POST http://localhost:5001/write \
		-H "Content-Type: application/json" \
		-d '{"message":"quorum test","level":"info"}' | python3 -m json.tool
	@echo "\n=== Wait for replication ==="
	sleep 3
	@echo "\n=== Files on all nodes ==="
	curl -sf http://localhost:5001/files | python3 -m json.tool
	curl -sf http://localhost:5002/files | python3 -m json.tool
	curl -sf http://localhost:5003/files | python3 -m json.tool
	@echo "\n=== Stop node3 ==="
	docker stop storage-node3
	@echo "Waiting for health checks to detect failure..."
	sleep 35
	@echo "\n=== Cluster status (node3 should be unhealthy) ==="
	curl -sf http://localhost:5001/cluster/status | python3 -m json.tool
	@echo "\n=== Write should still work (2/3 quorum) ==="
	curl -sf -X POST http://localhost:5001/write \
		-H "Content-Type: application/json" \
		-d '{"message":"degraded write","level":"warn"}' | python3 -m json.tool
	@echo "\n=== Restart node3 ==="
	docker start storage-node3
	sleep 5
	@echo "\n=== Cluster status (all healthy again) ==="
	curl -sf http://localhost:5001/cluster/status | python3 -m json.tool
	docker compose down -v
	@echo "\n=== E2E PASSED ==="

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

logs:
	docker compose logs -f

loadtest:
	@echo "Load tests - to be implemented in Commit 9"
