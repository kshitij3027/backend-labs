.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int clean

help:
	@echo "Real-Time Log Pattern Clustering — make targets:"
	@echo "  build      - build the app + test images"
	@echo "  up         - run the app live on http://localhost:8000 (detached)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the app logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

up:
	docker compose up -d --build app
	@echo "API: http://localhost:8000"

down:
	docker compose down

logs:
	docker compose logs -f app

# Rebuild the tester image first so a stale image can never mask a code change, then
# run the full suite in a throwaway container.
test:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/unit -q

test-int:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/integration -q

clean:
	docker compose down -v --remove-orphans
