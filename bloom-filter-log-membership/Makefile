.PHONY: help build up down logs logs-dashboard test test-unit test-int test-e2e e2e load clean

help:
	@echo "Targets:"
	@echo "  build          - build the app + tester images"
	@echo "  up             - start the API + dashboard (detached) on :8001 / :8002"
	@echo "  down           - stop and remove the stack"
	@echo "  logs           - tail app logs"
	@echo "  logs-dashboard - tail dashboard logs"
	@echo "  test           - run the full test suite in Docker"
	@echo "  test-unit      - run only tests/unit in Docker"
	@echo "  test-int       - run only tests/integration in Docker"
	@echo "  test-e2e       - run only tests/e2e in Docker"
	@echo "  e2e            - cross-container E2E verifier against the live stack"
	@echo "  load           - containerized load test (ops/sec, qps, latency, memory gates)"
	@echo "  clean          - down + remove volumes, orphans, and dangling images"

build:
	docker compose --profile test build

up:
	docker compose up -d app dashboard
	@echo "API:       http://localhost:8001"
	@echo "Dashboard: http://localhost:8002"

down:
	docker compose down

logs:
	docker compose logs -f app

logs-dashboard:
	docker compose logs -f dashboard

test:
	docker compose --profile test run --rm test pytest -v

test-unit:
	docker compose --profile test run --rm test pytest tests/unit -v

test-int:
	docker compose --profile test run --rm test pytest tests/integration -v

test-e2e:
	docker compose --profile test run --rm test pytest tests/e2e -v

# `run` starts the depends_on services (app + dashboard) and waits for their
# healthchecks; the verifier's exit code is captured, the stack is torn down,
# and that code is re-raised so CI / the caller sees the true result.
e2e:
	docker compose --profile e2e run --build --rm e2e; ec=$$?; docker compose down; exit $$ec

load:
	docker compose --profile loadtest run --build --rm loadtest; ec=$$?; docker compose down; exit $$ec

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
