.DEFAULT_GOAL := help
# `e2e` and `load` were declared here from C1 even though their scripts land in C13/C14. The
# alternative — leaving them out until then — makes `make e2e` fail with "No rule to make target",
# which reads as a broken Makefile rather than as "not yet". So both targets existed from the
# start, printed exactly which commit brings them, and exited 0: a scheduled feature must not turn
# a green build red. `e2e` became the real invocation in C13 and `load` is one as of THIS commit —
# each placeholder replaced itself exactly as its comment said it would, and `make help` never had
# to lie about what this project is going to have.
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
	@echo "  e2e        - black-box E2E verifier vs the live stack (incl. the double-spend check)"
	@echo "  load       - perf/load gates vs the live stack (throughput, overhead, scaling, memory)"
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

# Black-box E2E verifier (scripts/verify_e2e.py) against the LIVE stack, including the distributed
# double-spend check that is the whole reason this project exists.
#
# Torn down with -v FIRST, for the reason `make test` gives and one more that is specific to this
# target: the redis-data volume survives a plain `down`, so yesterday's daily QUOTA counters and any
# tier left resized by an interrupted run would still be there. Every count this verifier asserts is
# exact, so it starts from an empty store or it is grading against state nobody recorded.
#
# `up --wait lb` names ONE service and the whole topology follows from its depends_on (nginx waits
# on both replicas being healthy, each of those waits on `redis-cli ping`). --wait is what makes the
# verifier's own /health poll a formality rather than the thing standing between it and a race.
#
# The verifier runs in a throwaway container (--build, so a stale image can never mask a change) and
# its exit status is captured and re-raised AFTER the teardown — one failed check fails `make e2e`
# and still leaves a clean machine behind.
#
# Every gate is host-overridable, which is how we prove they bite instead of decorate:
#   MAX_OVERHEAD_MS=0 make e2e        MUST exit non-zero
#   E2E_BURST_REQUESTS=1 make e2e     MUST exit non-zero (a burst of 1 proves nothing)
# `--profile loadtest` on the PRE-run teardown only, and it is about `make load`'s `api-baseline`
# rather than about this target: an interrupted load run can leave that unmetered replica running,
# and a container that survives into this `up` holds an IP while api1/api2 are recreated on new ones
# — which is exactly how nginx ends up proxying to it (see the `load` target). Removing it first
# costs nothing here and removes the whole class of confusion.
e2e:
	docker compose --profile loadtest down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose up -d --build --wait lb
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down -v; \
	exit $$status

# Perf/load harness (scripts/load_test.py) against the LIVE stack: the spec's 100-parallel-request
# test, throughput, the limiter's added latency, scaling across replicas, and memory. The
# placeholder this replaces said it would become the real invocation in C14, and here it is.
#
# Same shape as `e2e` — down -v first, bring the stack up, run the harness in a throwaway
# container, capture the status, tear down, re-raise — and for the same reasons. The starting store
# must be empty (`down -v`, dropping the redis-data volume): the harness fires ~62k requests across
# ~125 freshly-minted principals, and leftover quota counters from an interrupted run are exactly
# the state that turns a throughput phase into a 429 phase.
#
# .. rubric:: Why `--profile loadtest` is on EVERY compose command here, and why `api-baseline`
#             comes up in the same `up` as `lb`
#
# This target starts one extra process — `api-baseline`, an `api1` with RATE_LIMIT_ENABLED=false
# that the overhead phase measures against — and nginx resolves the names in its `upstream` block
# EXACTLY ONCE, at startup. Those two facts collide, and the collision was observed rather than
# imagined:
#
#   * `down -v` WITHOUT the profile leaves `api-baseline` running (compose only tears down services
#     in the active profiles), so an unmetered replica survives `make load` — hence the profile on
#     both `down`s;
#   * a surviving container then holds its IP while the next `up` recreates api1/api2 on NEW ones,
#     and nginx — pointed at the old addresses — proxied the entire load run to the UNMETERED
#     baseline. Every request came back 200, phase A printed `Successful: 100, Rate Limited: 0`,
#     and the whole run measured a service with no rate limiter in it.
#
# So: `build` first (no image is built after nginx exists), then ONE `up` that creates
# `api-baseline` in the same wave as api1/api2 — both only wait on redis, while `lb` waits for the
# replicas to be *healthy*, so the proxy is necessarily the last container created and resolves
# addresses that nothing will move afterwards. `run` gets no `--build` for the same reason: a
# rebuild there can recreate the very containers nginx has already resolved.
#
# The harness re-establishes all of this at runtime rather than trusting this comment: it probes
# `/health` through the LB and refuses to start unless the hosts answering are exactly the replicas
# and never the baseline.
#
# Every gate is host-overridable, which is how we prove they bite instead of decorate:
#   LOAD_MIN_RPS=1000000 make load     MUST exit non-zero
#   LOAD_MAX_OVERHEAD_MS=0 make load   MUST exit non-zero
#   MAX_BACKEND_MEM_MB=1 make load     MUST exit non-zero
load:
	docker compose --profile loadtest down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose --profile loadtest build
	docker compose --profile loadtest up -d --wait lb api-baseline
	@status=0; \
	docker compose --profile loadtest run --rm loadtest || status=$$?; \
	docker compose --profile loadtest down -v; \
	exit $$status

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
#
# `--profile loadtest` because a plain `down` only tears down services in the ACTIVE profiles, and
# `make load`'s `api-baseline` (an api replica with RATE_LIMIT_ENABLED=false) would otherwise
# survive an interrupted run and sit there unmetered — see the `load` target for what happened when
# it did. `clean` is the target whose whole job is "leave nothing behind", so it names the profile.
clean:
	docker compose --profile loadtest down -v --remove-orphans
