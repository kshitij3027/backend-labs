.PHONY: build test run clean verify e2e

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	docker compose up app

verify:
	docker compose build test
	docker compose run --rm --entrypoint bash test /app/verify.sh

e2e:
	@echo "=== Building ==="
	docker compose build app
	@echo ""
	@echo "=== Starting web UI ==="
	docker compose up -d app
	@echo "Waiting for service to start..."
	@sleep 3
	@echo ""
	@echo "=== Health check ==="
	curl -sf http://localhost:8080/health | python3 -m json.tool
	@echo ""
	@echo "=== Sample API ==="
	curl -sf http://localhost:8080/api/sample | python3 -c "import sys, json; d=json.load(sys.stdin); print(f\"Sample: {d['count']} entries parsed\")"
	@echo ""
	@echo "=== Config API ==="
	curl -sf http://localhost:8080/api/config | python3 -c "import sys, json; d=json.load(sys.stdin); print(f\"Adapters: {[a['name'] for a in d['adapters']]}\")"
	@echo ""
	@echo "=== Upload API ==="
	curl -sf -X POST http://localhost:8080/api/upload --form-string "text=<34>Oct 11 22:14:15 mymachine su: test message" --form-string "format=json" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f\"Upload: {d['count']} entries parsed\")"
	@echo ""
	@echo "=== Cleanup ==="
	docker compose down
	@echo ""
	@echo "=== E2E TESTS PASSED ==="

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
