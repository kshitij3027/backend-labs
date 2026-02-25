.PHONY: build run test e2e clean

build:
	docker compose build

run:
	docker compose up -d app

test:
	docker compose build tests
	docker compose run --rm tests

e2e:
	@echo "=== Building and starting app ==="
	docker compose up -d --build app
	@echo "=== Waiting for health check ==="
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:8080/health > /dev/null 2>&1; then \
			echo "Health check passed"; \
			break; \
		fi; \
		echo "Waiting... ($$i)"; \
		sleep 2; \
	done
	@echo "=== Running E2E tests ==="
	@echo "1. Health check"
	curl -sf http://localhost:8080/health | python3 -m json.tool
	@echo ""
	@echo "2. Register schema"
	curl -sf -X POST http://localhost:8080/schemas \
		-H "Content-Type: application/json" \
		-d '{"subject":"e2e-test","schema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}' | python3 -m json.tool
	@echo ""
	@echo "3. Validate valid data"
	curl -sf -X POST http://localhost:8080/validate \
		-H "Content-Type: application/json" \
		-d '{"subject":"e2e-test","data":{"name":"Alice"}}' | python3 -m json.tool
	@echo ""
	@echo "4. Validate invalid data"
	curl -sf -X POST http://localhost:8080/validate \
		-H "Content-Type: application/json" \
		-d '{"subject":"e2e-test","data":{"name":123}}' | python3 -m json.tool
	@echo ""
	@echo "5. Check compatibility"
	curl -sf -X POST http://localhost:8080/compatibility/subjects/e2e-test \
		-H "Content-Type: application/json" \
		-d '{"schema":{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}}' | python3 -m json.tool
	@echo ""
	@echo "6. Dedup check (re-register same schema)"
	curl -sf -X POST http://localhost:8080/schemas \
		-H "Content-Type: application/json" \
		-d '{"subject":"e2e-test","schema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}' | python3 -m json.tool
	@echo ""
	@echo "7. List subjects"
	curl -sf http://localhost:8080/schemas/subjects | python3 -m json.tool
	@echo ""
	@echo "8. Metrics"
	curl -sf http://localhost:8080/metrics | python3 -m json.tool
	@echo ""
	@echo "9. Dashboard check"
	curl -sf -o /dev/null -w "Dashboard HTTP status: %{http_code}\n" http://localhost:8080/
	@echo ""
	@echo "=== E2E tests complete ==="
	docker compose down

clean:
	docker compose down -v --rmi local
	docker system prune -f
