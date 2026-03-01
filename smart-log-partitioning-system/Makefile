.PHONY: build test run stop e2e demo clean logs

HOST_PORT ?= 5050

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	HOST_PORT=$(HOST_PORT) docker compose up -d app
	@echo "App running at http://localhost:$(HOST_PORT)"

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
	@echo "--- Ingesting test logs ---"
	curl -sf -X POST http://localhost:$(HOST_PORT)/api/ingest \
		-H "Content-Type: application/json" \
		-d '[{"source":"web_server","level":"info","message":"test1","timestamp":"2026-02-28T10:00:00"},{"source":"database","level":"error","message":"test2","timestamp":"2026-02-28T11:00:00"},{"source":"auth","level":"warn","message":"test3","timestamp":"2026-02-28T12:00:00"}]' | python3 -m json.tool
	@echo ""
	@echo "--- Query by source ---"
	curl -sf "http://localhost:$(HOST_PORT)/api/query?source=web_server" | python3 -m json.tool
	@echo ""
	@echo "--- Stats ---"
	curl -sf http://localhost:$(HOST_PORT)/api/stats | python3 -m json.tool
	@echo ""
	@echo "--- Dashboard Check ---"
	curl -sf -o /dev/null -w "Dashboard HTTP status: %{http_code}\n" http://localhost:$(HOST_PORT)/
	@echo ""
	@echo "=== E2E Tests Complete ==="
	docker compose down

demo: build
	docker compose run --rm app python -m src demo --count 1000 --nodes 3 --strategy source

clean:
	docker compose down -v --rmi local
	rm -rf data/

logs:
	docker compose logs -f app
