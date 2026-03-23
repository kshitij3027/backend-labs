.PHONY: build run stop test e2e logs clean

build:
	docker compose build

run:
	docker compose up --build -d

stop:
	docker compose down

test:
	docker compose --profile test up --build --abort-on-container-exit test

e2e:
	@echo "--- Starting services ---"
	docker compose up --build -d
	@echo "--- Waiting for coordinator to be healthy ---"
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:8000/health > /dev/null 2>&1; then \
			echo "Coordinator is healthy"; \
			break; \
		fi; \
		echo "Waiting... ($$i)"; \
		sleep 2; \
	done
	@echo "--- Running E2E tests ---"
	@echo "POST /jobs"
	@curl -sf -X POST http://localhost:8000/jobs \
		-H "Content-Type: application/json" \
		-d '{"input_path": "/data/sample-logs.jsonl", "map_fn": "word_count", "reduce_fn": "sum"}' | python -m json.tool
	@echo ""
	@echo "GET /jobs"
	@curl -sf http://localhost:8000/jobs | python -m json.tool
	@echo ""
	@echo "GET /health"
	@curl -sf http://localhost:8000/health | python -m json.tool
	@echo ""
	@echo "--- E2E tests passed ---"

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local --remove-orphans
