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
	@echo "  build      - build all images (api1/api2 + test)"
	@echo "  up         - run Redis + BOTH api replicas behind the nginx load balancer (detached)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the load balancer + both replica logs, interleaved"
	@echo "  test       - full pytest suite in Docker, with the >=90% branch coverage gate"
	@echo "  test-unit  - unit tests only in Docker (no coverage gate — see below)"
	@echo "  test-int   - integration tests only in Docker (no coverage gate — see below)"
	@echo "  e2e        - black-box E2E verifier vs the live stack (arrives in C13)"
	@echo "  load       - perf/load gates vs the live stack (arrives in C14)"
	@echo "  ui         - run the stack (detached) and print the dashboard URL"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring the stack up detached. ONE service is named — `lb` — and the whole topology follows from
# its depends_on: nginx waits on `api1` and `api2` being *healthy*, and each of those waits on
# Redis answering `redis-cli ping`. So the API never begins life in the degraded fail-open path,
# and nginx never starts against an upstream name Docker's DNS cannot resolve yet (it resolves the
# `upstream` block once, at startup, and exits rather than retrying). --build keeps a stale image
# from silently serving yesterday's code.
#
# The printed URL is the LOAD BALANCER's, and it is the only way in from the host: neither replica
# publishes a port. That is the point of C12 — the default way to use this project is the
# distributed one, so a `curl` from the README exercises the configuration whose correctness was
# actually in question.
#
# The replica count is COUNTED from the running stack rather than printed as a literal "2". It has
# to agree with API_REPLICAS (which sizes each replica's degraded fallback bucket at
# ceil(capacity / API_REPLICAS)), and a number that is typed in two places is a number that will
# eventually disagree with itself — silently, and only during an outage.
up:
	docker compose up -d --build lb
	@echo "API:      http://localhost:$${API_PORT:-8020}  (GET /health — through the load balancer)"
	@echo "Docs:     http://localhost:$${API_PORT:-8020}/docs"
	@echo "Replicas: $$(docker compose ps --services --status running | grep -cE '^api[0-9]+$$') behind nginx (round-robin over api1:8000, api2:8000)"
	@echo "Proof:    for i in \$$(seq 1 20); do curl -s localhost:$${API_PORT:-8020}/health | python3 -c 'import sys,json;print(json.load(sys.stdin)[\"served_by\"])'; done | sort | uniq -c"

down:
	docker compose down

# All three, interleaved, because the interesting line is a request appearing in the LB's log with
# an `upstream=` field and then in exactly ONE replica's log. Tailing `api1` alone shows half the
# traffic and reads as if the service were dropping requests.
logs:
	docker compose logs -f lb api1 api2

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
# tests/integration/ exists as of C2, so the `if [ -d ... ]` guard C1 carried has removed itself
# exactly as its comment said it would, and this is back to the plain house form.
test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/integration || status=$$?; \
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

# The dashboard is served by the SAME uvicorn processes on the SAME host port as the API (C15 adds
# `GET /` and `GET /dashboard/` -> src/static/index.html). There is no second service and no node
# image, so `ui` is exactly `up` plus the extra URL. The URL 404s until C15 lands.
#
# Note for C15, since it is a consequence of this commit rather than of that one: the dashboard is
# load balanced like everything else, so consecutive polls of `/dashboard/api/stats` land on
# DIFFERENT replicas and the per-process counters in that payload (limiter checks, cache hit rate,
# uptime) will alternate between two series. `served_by` in the payload is what disambiguates
# them, and the replica fan-out strip is the feature that turns the artefact into the point.
ui: up
	@echo "Dashboard: http://localhost:$${API_PORT:-8020}/dashboard/  (arrives in C15)"

# -v drops the redis-data volume too, so the next run starts from an empty store: leftover quota
# counters from yesterday are exactly the kind of state that makes a limiter test pass for the
# wrong reason.
clean:
	docker compose down -v --remove-orphans
