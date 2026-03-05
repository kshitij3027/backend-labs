.PHONY: build test run stop e2e clean logs

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	docker compose up --build -d coordinator
	@echo "Services running. Coordinator: http://localhost:8080"

stop:
	docker compose down

e2e: build
	@echo "=== Starting E2E Tests ==="
	docker compose up -d coordinator
	@echo "Waiting for services..."
	@sleep 5
	@echo "--- Health Checks ---"
	curl -sf http://localhost:8080/health | python3 -m json.tool
	curl -sf http://localhost:8081/health | python3 -m json.tool
	curl -sf http://localhost:8082/health | python3 -m json.tool
	@echo ""
	@echo "--- Basic Query ---"
	curl -sf -X POST http://localhost:8080/query -H "Content-Type: application/json" -d '{"limit": 5}' | python3 -m json.tool
	@echo ""
	@echo "=== E2E Tests Complete ==="
	docker compose down

clean:
	docker compose down -v --rmi local

logs:
	docker compose logs -f
