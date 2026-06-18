.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load clean

help:
	@echo "Adaptive Resource Allocation — make targets:"
	@echo "  build      - build the app + test images"
	@echo "  up         - run the app live on http://localhost:8080 (detached)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the app logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - bring app up (fast cooldown), run E2E verifier, tear down"
	@echo "  load       - bring app up, run load test, tear down"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

up:
	docker compose up -d --build app
	@echo "Dashboard + API: http://localhost:8080"

down:
	docker compose down

logs:
	docker compose logs -f app

# Rebuild the tester image first so a stale image can never mask a code change,
# then run the full suite in a throwaway container.
test:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/unit -q

test-int:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/integration -q

# Bring the app up with a fast scale-down cooldown so the autoscaler reacts
# within the E2E window, run the verifier against it by service name, capture the
# verifier's exit code, tear the stack down, then re-exit with that code so CI /
# the caller sees the true pass/fail.
e2e:
	COOLDOWN_PERIOD_SECONDS=2 docker compose up -d --build app
	docker compose --profile e2e run --build --rm e2e; ec=$$?; docker compose down; exit $$ec

# Same capture-exit-code-then-teardown pattern for the load test.
load:
	docker compose up -d --build app
	docker compose --profile loadtest run --build --rm loadtest; ec=$$?; docker compose down; exit $$ec

clean:
	docker compose down -v --remove-orphans
