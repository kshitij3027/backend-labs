.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load clean

help:
	@echo "Delta Encoding Log Engine — make targets:"
	@echo "  build      - build the tester image (docker compose --profile test build)"
	@echo "  up         - start the app (detached) on :8080 with a fresh build"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the app logs"
	@echo "  test       - run the full pytest suite in Docker"
	@echo "  test-unit  - run only tests/unit in Docker"
	@echo "  test-int   - run only tests/integration in Docker"
	@echo "  e2e        - bring app up, run the containerized E2E verifier, tear down"
	@echo "  load       - bring app up, run the containerized load test, tear down"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose --profile test build

up:
	docker compose up -d --build app
	@echo "Dashboard+API: http://localhost:8080"

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test run --rm test pytest tests/unit -q

test-int:
	docker compose --profile test run --rm test pytest tests/integration -q

# Bring the app up, run the verifier against it by service name, capture the
# verifier's exit code, tear the stack down, then re-exit with that code so CI /
# the caller sees the true pass/fail.
e2e:
	docker compose up -d --build app
	docker compose --profile e2e run --build --rm e2e; ec=$$?; docker compose down; exit $$ec

# Same pattern for the load test (compress >=1000 eps, processing >=100 eps,
# error rate <=1%, reconstruct p99 <100ms).
load:
	docker compose up -d --build app
	docker compose --profile loadtest run --build --rm loadtest; ec=$$?; docker compose down; exit $$ec

clean:
	docker compose down -v --remove-orphans
