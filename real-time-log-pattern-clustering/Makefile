.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load ui clean

help:
	@echo "Real-Time Log Pattern Clustering — make targets:"
	@echo "  build      - build the app + test images"
	@echo "  up         - run the app live on http://localhost:8000 (detached)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the app logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - full end-to-end verifier in Docker (black-box against live app)"
	@echo "  load       - latency + throughput gates in Docker (black-box against live app)"
	@echo "  ui         - run app + React dashboard on http://localhost:8080 (detached)"
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

# Full end-to-end verifier (Commit 13), black-box in Docker against the live app:
# bring up `app` (built), run the profile-gated `e2e` service (which waits for the app
# healthcheck, then runs scripts/verify_e2e.py through the whole flow), capture its exit
# code, tear the stack down, and exit with that code so a failed check fails `make`.
# Mirrors the `load` target.
e2e:
	docker compose up -d --build app
	@status=0; \
	docker compose --profile e2e run --rm e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Latency + throughput gates (Commit 13), black-box in Docker against the live app:
# bring up `app` (built), run the profile-gated `loadtest` service (which waits for the
# app healthcheck, then runs perf_test.py + load_test.py), capture its exit code, tear
# the stack down, and exit with that code so a failed gate fails `make`. perf_test
# enforces single-request p95 < 150ms and engine amortized < 10ms/log; load_test
# enforces throughput >= 1000 logs/s.
load:
	docker compose up -d --build app
	@status=0; \
	docker compose --profile load run --rm loadtest || status=$$?; \
	docker compose down; \
	exit $$status

# Bring up the backend + the nginx-served React dashboard. The dashboard waits
# for the app healthcheck before starting (see docker-compose.yml).
ui:
	docker compose up -d --build app dashboard
	@echo "Dashboard: http://localhost:8080"
	@echo "API:       http://localhost:8000"

clean:
	docker compose down -v --remove-orphans
