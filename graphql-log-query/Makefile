.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int sdl e2e load ui clean

help:
	@echo "GraphQL Log Query Platform — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the stack (detached) and print the API + GraphiQL URLs"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the API logs"
	@echo "  test       - full pytest suite in Docker (rebuilds the test image first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  sdl        - print the live GraphQL SDL (paste into schema.graphql when it drifts)"
	@echo "  e2e        - black-box E2E verifier vs the live stack"
	@echo "  load       - perf/load gates vs the live stack (throughput, latency, memory)"
	@echo "  ui         - run the stack (detached) and print the dashboard URL"
	@echo "  clean      - down + remove volumes and orphans"

# Both images, explicitly. `test` is profile-gated, and a bare `docker compose build` skips
# services whose profile is not active — naming the service builds it regardless, which is what
# makes this target actually prove BOTH Dockerfiles still build.
build:
	docker compose build api
	docker compose build test

# Bring the stack up detached: postgres and redis first (compose resolves depends_on), then the
# api once both report healthy. --build keeps a stale image from silently serving yesterday's
# code. The container always binds :8000; API_PORT is the HOST side only.
up:
	docker compose up -d --build api
	@echo "API:      http://localhost:$${API_PORT:-8020}         (GET /health)"
	@echo "GraphQL:  http://localhost:$${API_PORT:-8020}/graphql (POST; GET serves GraphiQL)"

down:
	docker compose down

logs:
	docker compose logs -f api

# Full suite. Torn down with -v FIRST so a leftover container or volume from an interrupted run
# can never leak state into the suite — and, specifically here, so the postgres init script that
# creates `gqllogs_test` is guaranteed to have run against the container the suite connects to
# (those scripts only fire on a first init of an EMPTY data directory). Then the tester image is
# rebuilt so a stale image cannot mask a code change. The exit status is captured and re-raised
# AFTER the teardown, so a failing test still fails `make test` and still leaves a clean machine.
#
# NOTE: no -q on any pytest invocation below. Verbosity is centralized in pytest.ini
# (`addopts = -q --tb=short`) so it cannot drift between targets; another -q here would stack to
# -qq, which suppresses the "N passed in Xs" summary line entirely.
test:
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest || status=$$?; \
	docker compose down -v; \
	exit $$status

# Unit tests only — the fast inner loop. Plain `down` (no -v): unit tests touch neither Postgres
# nor Redis, and skipping the volume prune keeps the loop quick.
test-unit:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/unit || status=$$?; \
	docker compose down; \
	exit $$status

# Integration tests only. `down -v` here, unlike the sibling project's plain `down`, because
# these DO talk to the real Postgres test database: leaving pgdata behind would let one run's
# tables decide the next run's starting state, which is the definition of a non-hermetic suite.
test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/integration || status=$$?; \
	docker compose down -v; \
	exit $$status

# Print the SDL the CURRENT code produces, to stdout, from inside the image. This is the companion
# to tests/unit/test_schema_sdl.py: when that test reports drift, the fix is to paste this output
# over schema.graphql — deliberately intended and reviewed, rather than auto-applied.
#
# --stdout, not a file write, because the container's filesystem is discarded on exit and there is
# no bind mount; a written file could never reach the repository. --no-deps because rendering a
# schema touches neither Postgres nor Redis, and waiting on two healthchecks to print a string is
# pure latency.
sdl:
	docker compose build test
	docker compose --profile test run --rm --no-deps test python -m scripts.export_sdl --stdout

# Black-box E2E: boot the stack fresh and WAIT on the api healthcheck (--wait), so the verifier
# can never race a still-seeding corpus, then run the verifier in a throwaway container (rebuilt
# so a stale image cannot mask a change) and tear the stack down. The verifier's exit status
# propagates, so one failed check fails `make e2e`.
# scripts/verify_e2e.py lands at C12; until then this target fails loudly, by design.
e2e:
	docker compose up -d --build --wait api
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down -v; \
	exit $$status

# Perf/load gates against the same live stack, same boot-wait-run-teardown pattern. Gates are
# host-overridable, e.g. `LOAD_MIN_RPS=1000000 make load` MUST exit non-zero — that is how we
# prove the gates bite instead of merely existing.
# scripts/load_test.py lands at C14; until then this target fails loudly, by design.
load:
	docker compose up -d --build --wait api
	@status=0; \
	docker compose --profile loadtest run --rm --build loadtest || status=$$?; \
	docker compose down -v; \
	exit $$status

# The dashboard is served by the SAME uvicorn process on the SAME port as the API — C13 builds
# the React SPA in a node:20-alpine stage and copies the bundle into the Python image, so there
# is no second service, no nginx sidecar and no separate port. That same-origin arrangement is
# also why the browser's GraphQL calls need no CORS preflight and its subscription WebSocket is
# not a cross-origin upgrade. So `ui` is exactly `up` plus the extra URL.
ui: up
	@echo "Dashboard: http://localhost:$${API_PORT:-8020}/   (SPA lands at C13)"

clean:
	docker compose down -v --remove-orphans
