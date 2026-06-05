.PHONY: help build up down logs test test-unit test-int test-e2e e2e load clean

help:
	@echo "Targets:"
	@echo "  build      - build the app + tester images"
	@echo "  up         - start the app (detached) at http://localhost:8000"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail app logs"
	@echo "  test       - run the full test suite in Docker"
	@echo "  test-unit  - run only tests/unit in Docker"
	@echo "  test-int   - run only tests/integration in Docker"
	@echo "  test-e2e   - run only tests/e2e in Docker"
	@echo "  e2e        - cross-container E2E verifier against the live stack"
	@echo "  load       - containerized load test (ingest throughput + query p90 gates)"
	@echo "  clean      - down + remove volumes"

build:
	docker compose --profile test build

up:
	docker compose up -d app

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm test pytest -v

test-unit:
	docker compose --profile test run --rm test pytest tests/unit -v

test-int:
	docker compose --profile test run --rm test pytest tests/integration -v

test-e2e:
	docker compose --profile test run --rm test pytest tests/e2e -v

e2e:
	docker compose --profile e2e run --build --rm e2e; ec=$$?; docker compose down; exit $$ec

load:
	docker compose --profile loadtest run --build --rm loadtest; ec=$$?; docker compose down; exit $$ec

clean:
	docker compose down -v
