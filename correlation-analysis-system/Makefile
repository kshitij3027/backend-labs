.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load ui clean

help:
	@echo "Correlation Analysis System — make targets:"
	@echo "  build      - build all images (backend + test)"
	@echo "  up         - run the runtime (backend, detached, with redis)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the backend logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - black-box E2E verifier vs the live stack (14 checks)"
	@echo "  load       - perf/load gates vs the live stack (ingest, latency, RPS, memory)"
	@echo "  ui         - run redis + backend + React dashboard (detached), print the URLs"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring up the backend detached (redis comes up as a healthy dep).
up:
	docker compose up -d --build backend
	@echo "API: http://localhost:$${BACKEND_PORT:-8000}  (GET /health)"

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

# Black-box E2E: boot redis + backend fresh (waiting on their healthchecks), run
# the 14-check verifier in a throwaway container (rebuilt so a stale image can
# never mask a change), tear the stack down; the verifier's exit status propagates
# so one failed check fails `make e2e`.
e2e:
	docker compose up -d --build --wait redis backend
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Perf/load gates against the same live stack, same exit-code pattern. Gates are
# host-overridable, e.g. `MIN_EVENTS_PER_SEC=100000 make load` must fail.
load:
	docker compose up -d --build --wait redis backend
	@status=0; \
	docker compose --profile loadtest run --rm --build loadtest || status=$$?; \
	docker compose down; \
	exit $$status

# Bring up the full dashboard stack (redis + backend + frontend) detached, building
# images as needed. Redis and backend come up first as healthy deps of the frontend;
# the SPA then polls GET /api/v1/dashboard through nginx's /api proxy every 5s.
ui:
	docker compose up -d --build redis backend frontend
	@echo "Dashboard: http://localhost:$${FRONTEND_PORT:-3000}"
	@echo "API:       http://localhost:$${BACKEND_PORT:-8000}  (GET /health)"

clean:
	docker compose down -v --remove-orphans
