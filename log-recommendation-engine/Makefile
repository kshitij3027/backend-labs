.DEFAULT_GOAL := help
.PHONY: help build up down logs migrate seed backfill ui dashboard scale test test-unit test-int e2e load clean

help:
	@echo "Log Recommendation Engine — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the runtime (api, detached, with postgres + redis)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the api logs"
	@echo "  migrate    - apply Alembic migrations against Postgres in Docker (no-op until C2)"
	@echo "  seed       - generate + ingest a synthetic incident corpus in Docker (from C3)"
	@echo "  backfill   - compute embeddings for NULL-embedded incidents in Docker (from C5)"
	@echo "  ui         - bring up postgres + redis + api + dashboard (detached); print the URL"
	@echo "  dashboard  - build + (re)start just the dashboard container (detached)"
	@echo "  scale      - scale the api behind the dashboard's nginx (from C23)"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - full end-to-end verifier in Docker (from C18)"
	@echo "  load       - perf (C19 p95 latency) + concurrent load (C20 RPS/error-rate) gates in Docker"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring up the API detached (postgres + redis come up as healthy deps).
up:
	docker compose up -d --build api
	@echo "API: http://localhost:$${API_PORT:-8000}  (GET /health)"

down:
	docker compose down

logs:
	docker compose logs -f api

# Apply migrations explicitly (the api entrypoint also does this on start, once
# alembic.ini lands in C2). Runs a throwaway api container that overrides the
# entrypoint to invoke alembic directly. No-op until C2 adds alembic.ini + migrations.
migrate:
	docker compose run --rm --entrypoint alembic api upgrade head

# Generate a synthetic incident corpus and ingest it. Runs a throwaway api
# container that shares Postgres with the running stack (the api entrypoint
# migrates on start; this one-off `run` hits the same migrated DB). Override args,
# e.g. `make seed ARGS="--count 200"`.
seed:
	docker compose run --rm api python -m scripts.seed $(ARGS)

# Backfill embeddings for any incidents left NULL-embedded (rows inserted before
# C5, or while the embedding service was unavailable). Runs a throwaway api
# container against the same migrated DB. Override args, e.g.
# `make backfill ARGS="--batch-size 128"`.
backfill:
	docker compose run --rm api python -m scripts.backfill_embeddings $(ARGS)

# Bring up the full runtime + web dashboard, detached. Compose starts postgres +
# redis (healthy deps of api), then api, then the dashboard once api is healthy.
# nginx inside the dashboard reverse-proxies /api → api:8000.
ui:
	docker compose up -d --build postgres redis api dashboard
	@echo "Dashboard: http://localhost:$${DASHBOARD_PORT:-8080}"
	@echo "API:       http://localhost:$${API_PORT:-8000}  (GET /health)"

# Build + (re)start just the dashboard container, detached (assumes api is up, or
# brings it up as a healthy dependency via compose).
dashboard:
	docker compose up -d --build dashboard
	@echo "Dashboard: http://localhost:$${DASHBOARD_PORT:-8080}"

# Horizontal API scaling behind the dashboard's nginx (C23).
#
# Layers docker-compose.scale.yml on top of the base file: the override empties the
# api's host-port publishing (`ports: !reset []`) so N>1 replicas don't collide on the
# single host port. `--scale api=$(N)` starts N api containers (works because api has NO
# container_name); `--wait` blocks until postgres, redis, api and dashboard are all
# healthy. Clients reach the load-balanced api ONLY through the dashboard's nginx, which
# re-resolves `api` per request via Docker DNS and round-robins across the replicas
# (frontend/nginx.conf). Override the replica count with `make scale N=5`.
#
# NOTE: :8080 (the default DASHBOARD_PORT) may already be taken on the host — run with
# an override, e.g. `DASHBOARD_PORT=8081 make scale N=3`, then hit :8081/api/health.
N ?= 3
scale:
	docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --build --wait \
		--scale api=$(N) postgres redis api dashboard
	@echo "Load-balanced API via dashboard: http://localhost:$${DASHBOARD_PORT:-8080}/api/health"
	@echo "Scaled api replicas: $(N)"

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

# Full black-box end-to-end verifier (C18) in Docker against the LIVE api.
#
# Builds + brings up postgres + redis + api and WAITS for them to be healthy
# (`--wait`), then runs the profile-gated `e2e` service (Dockerfile.test image) which
# executes scripts/verify_e2e.py — seeding a controlled corpus over HTTP and asserting
# the full loop (seed -> recommend -> feedback -> recommend -> re-rank shift), plus
# /stats and /metrics. The verifier's exit code is captured; the stack is torn down
# regardless; and `make e2e` exits with that code so ANY failed assertion fails the
# target (exit-code propagation). Determinism is guaranteed inside the verifier itself
# (it PUTs /config epsilon_explore=0 first), so no api env override is needed here.
e2e:
	docker compose up -d --build --wait postgres redis api
	@status=0; \
	docker compose --profile e2e run --rm --build e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Recommend-path perf + concurrent load gates (C19 + C20) in Docker against the LIVE api.
#
# Builds + brings up postgres + redis + api and WAITS for them to be healthy
# (`--wait`), then runs the profile-gated `loadtest` service (Dockerfile.test image)
# for BOTH scripts, in one throwaway container:
#   1. scripts/perf_test  (C19) — seed (if small), warm up (discard the MiniLM load),
#      time PERF_SAMPLES *serial* varied POST /recommend calls, HARD-GATE p95 under
#      PERF_MAX_RECOMMEND_MS  (single-request latency tail).
#   2. scripts/load_test  (C20) — drive LOAD_REQUESTS *concurrent* POST /recommend
#      (semaphore LOAD_CONCURRENCY), HARD-GATE throughput (RPS >= LOAD_MIN_RPS) and
#      error_rate (<= LOAD_MAX_ERROR_RATE); latency percentiles + a best-effort memory
#      figure are reported (memory NEVER gates).
# They run as `perf_test && load_test`, so the load test only runs if perf passed AND a
# failure in EITHER script makes the `sh -lc` exit non-zero. That status is captured; the
# stack is torn down regardless; and `make load` exits with it — so a blown p95 (C19) OR a
# blown throughput/error-rate gate (C20) fails the target (exit-code propagation).
load:
	docker compose up -d --build --wait postgres redis api
	@status=0; \
	docker compose --profile loadtest run --rm --build loadtest sh -lc "python -m scripts.perf_test && python -m scripts.load_test" || status=$$?; \
	docker compose down; \
	exit $$status

clean:
	docker compose down -v --remove-orphans
