.PHONY: help build up down logs test test-unit test-int test-e2e clean

help:
	@echo "Targets:"
	@echo "  build      - build the app + tester images"
	@echo "  up         - start the API (detached) at http://localhost:8001"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail app logs"
	@echo "  test       - run the full test suite in Docker"
	@echo "  test-unit  - run only tests/unit in Docker"
	@echo "  test-int   - run only tests/integration in Docker"
	@echo "  test-e2e   - run only tests/e2e in Docker"
	@echo "  clean      - down + remove volumes, orphans, and dangling images"

build:
	docker compose --profile test build

up:
	docker compose up -d app
	@echo "API: http://localhost:8001"

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

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
