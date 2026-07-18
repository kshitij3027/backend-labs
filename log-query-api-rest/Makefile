.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load ui clean

help:
	@echo "Log Query API (REST) — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the API (detached) and print the API + docs URLs"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the API logs"
	@echo "  test       - full pytest suite in Docker (rebuilds the test image first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - black-box E2E verifier vs the live API"
	@echo "  load       - perf/load gates vs the live API (throughput, latency, memory)"
	@echo "  ui         - run the API (detached) and print the dashboard URL"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring the API up detached. It is the only service: all state is in-process, so there is no
# database, no cache and no second container to start. --build keeps a stale image from
# silently serving yesterday's code.
up:
	docker compose up -d --build api
	@echo "API:  http://localhost:$${API_PORT:-8010}  (GET /health)"
	@echo "Docs: http://localhost:$${API_PORT:-8010}/docs"

down:
	docker compose down

logs:
	docker compose logs -f api

# Full suite. Torn down with -v FIRST so a leftover container/volume from an interrupted run
# can never leak state into the suite, then the tester image is rebuilt so a stale image can
# never mask a code change. The exit status is captured and re-raised AFTER the teardown, so
# a failing test still fails `make test` and still leaves a clean machine behind.
#
# NOTE: no -q on any pytest invocation below. Verbosity is centralized in pytest.ini
# (`addopts = -q --tb=short`) so it cannot drift between targets; adding another -q here would
# stack to -qq, which suppresses the "N passed in Xs" summary line entirely and leaves the
# verification report counting progress dots.
test:
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest || status=$$?; \
	docker compose down -v; \
	exit $$status

# Unit tests only — the fast inner loop. Plain `down` (no -v): nothing here writes volumes,
# and skipping the volume prune keeps the loop quick.
test-unit:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/unit || status=$$?; \
	docker compose down; \
	exit $$status

# Integration tests only — the HTTP surface against an injected Runtime (still hermetic; no
# live container is involved, which is what separates this from `make e2e`).
test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/integration || status=$$?; \
	docker compose down; \
	exit $$status

# Black-box E2E: boot the API fresh and WAIT on its healthcheck (--wait), so the verifier can
# never race a still-seeding store, then run the verifier in a throwaway container (rebuilt so
# a stale image can never mask a change) and tear the stack down. The verifier's exit status
# propagates, so one failed check fails `make e2e`.
e2e:
	docker compose up -d --build --wait api
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Perf/load gates against the same live API, same boot-wait-run-teardown pattern. Gates are
# host-overridable, e.g. `LOAD_MIN_RPS=1000000 make load` MUST exit non-zero — that is how we
# prove the gates bite instead of merely existing.
load:
	docker compose up -d --build --wait api
	@status=0; \
	docker compose --profile loadtest run --rm --build loadtest || status=$$?; \
	docker compose down; \
	exit $$status

# The dashboard is served by the SAME uvicorn process on the SAME port as the API (C13 adds
# `GET /` -> src/static/index.html). There is no second service and no node/nginx image, so
# `ui` is exactly `up` plus the extra URL.
ui: up
	@echo "Dashboard: http://localhost:$${API_PORT:-8010}/"

clean:
	docker compose down -v --remove-orphans
