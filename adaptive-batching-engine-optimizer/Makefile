.PHONY: help build up down logs test test-unit test-int test-e2e e2e improvement clean

help:
	@echo "Targets:"
	@echo "  build        - build the app + tester images"
	@echo "  up           - start the app (detached) at http://localhost:8000"
	@echo "  down         - stop and remove the stack"
	@echo "  logs         - tail app logs"
	@echo "  test         - run the full test suite in Docker"
	@echo "  test-unit    - run only tests/unit"
	@echo "  test-int     - run only tests/integration"
	@echo "  test-e2e     - run only tests/e2e"
	@echo "  e2e          - end-to-end probe via scripts/e2e.sh"
	@echo "  improvement  - run the throughput-improvement demo"
	@echo "  clean        - down + remove volumes + prune caches"

build:
	docker compose --profile test build

up:
	docker compose up -d app

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm tester pytest -v

test-unit:
	docker compose --profile test run --rm tester pytest tests/unit -v

test-int:
	docker compose --profile test run --rm tester pytest tests/integration -v

test-e2e:
	docker compose --profile test run --rm tester pytest tests/e2e -v

e2e:
	docker compose --profile test run --rm tester bash scripts/e2e.sh

improvement:
	docker compose --profile test run --rm tester python scripts/improvement.py

clean:
	docker compose down -v && find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true && rm -rf .pytest_cache htmlcov .coverage
