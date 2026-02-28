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
	@echo "=== All nodes healthy ==="
	curl -sf http://localhost:8080/api/cluster | python3 -m json.tool
	@echo "\n=== Write 10 logs via dashboard ==="
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		curl -sf -X POST http://localhost:8080/api/write \
			-H "Content-Type: application/json" \
			-d "{\"message\":\"e2e test $$i\",\"level\":\"info\",\"index\":$$i}" > /dev/null; \
	done
	@echo "Written 10 logs"
	sleep 3
	@echo "\n=== Files across cluster ==="
	curl -sf http://localhost:8080/api/files | python3 -m json.tool
	@echo "\n=== Stop node3 — test degraded mode ==="
	docker stop storage-node3
	sleep 35
	@echo "\n=== Cluster status (degraded) ==="
	curl -sf http://localhost:8080/api/cluster | python3 -m json.tool
	@echo "\n=== Write in degraded mode ==="
	curl -sf -X POST http://localhost:5001/write \
		-H "Content-Type: application/json" \
		-d '{"message":"degraded write","level":"warn"}' | python3 -m json.tool
	@echo "\n=== Restart node3 ==="
	docker start storage-node3
	sleep 15
	@echo "\n=== Cluster status (recovered) ==="
	curl -sf http://localhost:8080/api/cluster | python3 -m json.tool
	docker compose down -v
	@echo "\n=== E2E PASSED ==="

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

logs:
	docker compose logs -f

loadtest:
	docker compose down -v 2>/dev/null || true
	docker compose up --build -d node1 node2 node3
	sleep 5
	docker compose build load-test
	docker compose run --rm load-test
	docker compose down -v
