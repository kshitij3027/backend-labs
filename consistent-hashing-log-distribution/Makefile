.PHONY: build test run stop e2e demo clean logs

HOST_PORT ?= 8080

build:
	docker compose build

test:
	docker build -t consistent-hashing-test -f Dockerfile.test .
	docker run --rm consistent-hashing-test

run:
	HOST_PORT=$(HOST_PORT) docker compose up -d app
	@echo "Dashboard running at http://localhost:$(HOST_PORT)"

stop:
	docker compose down

e2e: build
	@echo "=== Starting E2E Tests ==="
	HOST_PORT=$(HOST_PORT) docker compose up -d app
	@echo "Waiting for app to be healthy..."
	@sleep 3
	@echo "--- Health Check ---"
	curl -sf http://localhost:$(HOST_PORT)/health | python3 -m json.tool
	@echo ""
	@echo "--- Store logs ---"
	curl -sf -X POST http://localhost:$(HOST_PORT)/api/logs \
		-H "Content-Type: application/json" \
		-d '[{"source":"web-server","level":"info","message":"test log 1"},{"source":"database","level":"error","message":"test log 2"},{"source":"auth-service","level":"warning","message":"test log 3"}]' | python3 -m json.tool
	@echo ""
	@echo "--- Stats ---"
	curl -sf http://localhost:$(HOST_PORT)/api/stats | python3 -m json.tool
	@echo ""
	@echo "--- Add node ---"
	curl -sf -X POST http://localhost:$(HOST_PORT)/api/nodes \
		-H "Content-Type: application/json" \
		-d '{"node_id":"node4"}' | python3 -m json.tool
	@echo ""
	@echo "--- Simulate 100 logs ---"
	curl -sf -X POST http://localhost:$(HOST_PORT)/api/simulate \
		-H "Content-Type: application/json" \
		-d '{"count":100}' | python3 -m json.tool
	@echo ""
	@echo "--- Post-scaling stats ---"
	curl -sf http://localhost:$(HOST_PORT)/api/stats | python3 -m json.tool
	@echo ""
	@echo "--- Ring info ---"
	curl -sf http://localhost:$(HOST_PORT)/api/ring | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'VNodes: {len(d[\"vnodes\"])}, Nodes: {list(d[\"node_colors\"].keys())}')"
	@echo ""
	@echo "--- Remove node4 ---"
	curl -sf -X DELETE http://localhost:$(HOST_PORT)/api/nodes/node4 | python3 -m json.tool
	@echo ""
	@echo "--- Dashboard Check ---"
	curl -sf -o /dev/null -w "Dashboard HTTP status: %{http_code}\n" http://localhost:$(HOST_PORT)/
	@echo ""
	@echo "=== E2E Tests Complete ==="
	docker compose down

demo: build
	docker compose run --rm app demo --count 10000 --nodes 3

clean:
	docker compose down -v --rmi local

logs:
	docker compose logs -f app
