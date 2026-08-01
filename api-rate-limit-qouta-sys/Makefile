.DEFAULT_GOAL := help
# `e2e` and `load` are declared here from C1 even though their scripts land in C13/C14. The
# alternative — leaving them out until then — makes `make e2e` fail with "No rule to make target",
# which reads as a broken Makefile rather than as "not yet". So both targets exist now, print
# exactly which commit brings them, and exit 0: a scheduled feature must not turn a green build
# red. They are replaced by the real invocations in C13/C14, and `make help` never has to lie
# about what this project is going to have.
.PHONY: help build up down logs test test-unit test-int e2e load ui clean

help:
	@echo "API Rate Limiter & Quota Manager — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run Redis + the API (detached) and print the API + docs URLs"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the API logs"
	@echo "  test       - full pytest suite in Docker, with the >=90% branch coverage gate"
	@echo "  test-unit  - unit tests only in Docker (no coverage gate — see below)"
	@echo "  test-int   - integration tests only in Docker (no coverage gate — see below)"
	@echo "  e2e        - black-box E2E verifier vs the live stack (arrives in C13)"
	@echo "  load       - perf/load gates vs the live stack (arrives in C14)"
	@echo "  ui         - run the stack (detached) and print the dashboard URL"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring the stack up detached. `redis` starts implicitly via `api`'s depends_on and is waited on
# by its own healthcheck, so the API never begins life in the degraded fail-open path. --build
# keeps a stale image from silently serving yesterday's code.
up:
	docker compose up -d --build api
	@echo "API:  http://localhost:$${API_PORT:-8020}  (GET /health)"
	@echo "Docs: http://localhost:$${API_PORT:-8020}/docs"

down:
	docker compose down

logs:
	docker compose logs -f api

# Full suite + the coverage gate. Torn down with -v FIRST so a leftover container or Redis volume
# from an interrupted run can never leak counters into the suite, then the tester image is
# rebuilt so a stale image can never mask a code change. The exit status is captured and
# re-raised AFTER the teardown, so a failing test still fails `make test` and still leaves a
# clean machine behind.
#
# The coverage flags live HERE and nowhere else. COV_MIN is host-overridable precisely so the
# gate can be proven to bite: `COV_MIN=99 make test` MUST exit non-zero.
#
# NOTE: no -q on any pytest invocation below. Verbosity is centralized in pytest.ini
# (`addopts = -q --tb=short`) so it cannot drift between targets; adding another -q here would
# stack to -qq, which suppresses the "N passed in Xs" summary line entirely.
test:
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest --cov=src --cov-branch --cov-report=term-missing --cov-fail-under=$${COV_MIN:-90} || status=$$?; \
	docker compose down -v; \
	exit $$status

# Unit tests only — the fast inner loop.
#
# Deliberately NO coverage flags. A coverage percentage computed over the unit subset alone is a
# coverage-shaped lie: it reports the fraction of src/ that the unit tests happen to touch, while
# the number everyone reads it as ("how much of this project is tested") includes the integration
# suite, which is where every Lua and Redis-facing assertion lives. Publishing that smaller number
# would make the gate look tighter than it is; publishing it and passing would be worse. One
# measurement, from `make test`, over the whole suite.
#
# Plain `down` (no -v): skipping the volume prune keeps the loop quick.
test-unit:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/unit || status=$$?; \
	docker compose down; \
	exit $$status

# Integration tests only — the HTTP surface and the Lua scripts against the real redis:7-alpine
# service over the compose network (still hermetic in the sense that no published host port is
# involved, which is what separates this from `make e2e`). Same no-coverage rule as test-unit.
#
# The `if [ -d ... ]` guard exists because tests/integration/ arrives in C2: a target that `make
# help` advertises must not exit 4 with "file or directory not found" just because the suite it
# names lands in the next commit. The guard removes itself the moment the directory exists.
test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test \
	  sh -c 'if [ -d tests/integration ]; then pytest tests/integration; else echo "no integration tests yet — they arrive in C2"; fi' || status=$$?; \
	docker compose down; \
	exit $$status

# Black-box E2E verifier (scripts/verify_e2e.py). Arrives in C13, where this becomes the usual
# boot-wait-run-teardown against the live load balancer, including the distributed double-spend
# check that is the whole reason this project exists. Exits 0 today — see the .PHONY note above.
e2e:
	@echo "make e2e: the black-box E2E verifier arrives in C13 (scripts/verify_e2e.py)."
	@echo "          Until then, 'make test' covers the suite and 'make up' serves the API."

# Perf/load harness (scripts/load_test.py). Arrives in C14 with the throughput, limiter-overhead,
# scaling and memory gates, each host-overridable so an impossible bar proves the gate bites.
load:
	@echo "make load: the perf/load harness arrives in C14 (scripts/load_test.py)."
	@echo "           Until then, 'make test' covers the suite and 'make up' serves the API."

# The dashboard is served by the SAME uvicorn process on the SAME port as the API (C15 adds
# `GET /` and `GET /dashboard/` -> src/static/index.html). There is no second service and no
# node image, so `ui` is exactly `up` plus the extra URL. The URL 404s until C15 lands.
ui: up
	@echo "Dashboard: http://localhost:$${API_PORT:-8020}/dashboard/  (arrives in C15)"

# -v drops the redis-data volume too, so the next run starts from an empty store: leftover quota
# counters from yesterday are exactly the kind of state that makes a limiter test pass for the
# wrong reason.
clean:
	docker compose down -v --remove-orphans
