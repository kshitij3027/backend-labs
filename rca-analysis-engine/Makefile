.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load clean

help:
	@echo "RCA Analysis Engine — make targets:"
	@echo "  build      - build all images (backend + test)"
	@echo "  up         - run the backend (detached) and print the API URL"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the backend logs"
	@echo "  test       - full pytest suite in Docker (rebuilds the test image first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - black-box E2E verifier vs the live backend (11 checks)"
	@echo "  load       - perf/load gates vs the live backend (throughput, latency, memory)"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring up the backend detached. All state is in-memory, so there are no other
# services to start (the React dashboard is added in a later commit).
up:
	docker compose up -d --build backend
	@echo "API: http://localhost:$${BACKEND_PORT:-8000}  (GET /api/health)"

down:
	docker compose down

logs:
	docker compose logs -f backend

# Rebuild the tester image first so a stale image can never mask a code change, then
# run the suite in a throwaway container. Exit status propagates so a failed test
# fails `make test`; the stack is torn down afterwards regardless.
test:
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest -q || status=$$?; \
	docker compose down -v; \
	exit $$status

test-unit:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/unit -q || status=$$?; \
	docker compose down; \
	exit $$status

test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/integration -q || status=$$?; \
	docker compose down; \
	exit $$status

# Black-box E2E: boot the backend fresh (waiting on its healthcheck), run the 11-check
# verifier in a throwaway container (rebuilt so a stale image can never mask a change),
# tear the stack down; the verifier's exit status propagates so one failed check fails
# `make e2e`.
e2e:
	docker compose up -d --build --wait backend
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Perf/load gates against the same live backend, same exit-code pattern. Gates are
# host-overridable, e.g. `MIN_EVENTS_PER_SEC=100000 make load` must fail.
load:
	docker compose up -d --build --wait backend
	@status=0; \
	docker compose --profile loadtest run --rm --build loadtest || status=$$?; \
	docker compose down; \
	exit $$status

clean:
	docker compose down -v --remove-orphans
