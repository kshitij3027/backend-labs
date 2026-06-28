.DEFAULT_GOAL := help
.PHONY: help build up up-all worker beat dashboard ui scale down logs migrate seed test test-unit test-int e2e load clean

help:
	@echo "Predictive Log Analytics Engine — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the full runtime (api + worker + beat, detached, with postgres + redis)"
	@echo "  up-all     - alias for up (api + worker + beat)"
	@echo "  worker     - start the Celery worker only (detached)"
	@echo "  beat       - start the Celery beat scheduler only (detached)"
	@echo "  dashboard  - build + start the React dashboard (nginx) only (detached)"
	@echo "  ui         - bring up api + dashboard (detached); open http://localhost:8080"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the api logs"
	@echo "  migrate    - apply Alembic migrations against Postgres in Docker"
	@echo "  seed       - generate + ingest a synthetic metric dataset in Docker"
	@echo "  scale      - scale the api behind the dashboard's nginx (Docker DNS round-robin)"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - full end-to-end verifier in Docker (black-box against live api)"
	@echo "  load       - latency + throughput + memory gates in Docker (black-box)"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring up the full runtime: api + worker + beat (postgres + redis come up as deps).
up:
	docker compose up -d --build api worker beat
	@echo "API: http://localhost:8000 (worker + beat running)"

# Explicit alias for the full stack.
up-all: up

# Start the Celery worker on its own (builds if needed; deps come up automatically).
worker:
	docker compose up -d --build worker

# Start the Celery beat scheduler on its own (only ONE beat instance should run).
beat:
	docker compose up -d --build beat

# Build + start the React dashboard (served by nginx). Its api dependency comes
# up automatically (depends_on api: service_healthy).
dashboard:
	docker compose up -d --build dashboard

# Bring up the API + dashboard together (postgres + redis come up as deps).
ui:
	docker compose up -d --build api dashboard
	@echo "Dashboard: http://localhost:8080  (API: http://localhost:8000)"

# Horizontal scaling (bonus: "scale API servers behind a load balancer").
#
# The dashboard's nginx reverse-proxies /api -> api:8000 by service NAME, re-resolving
# the name PER REQUEST via Docker's embedded DNS (see frontend/nginx.conf). When the
# api is scaled to N replicas, Docker DNS resolves `api` to all N container IPs and
# nginx round-robins across them — no extra load-balancer container needed. The
# browser only ever talks to the dashboard on :8080.
#
# Host-port caveat: the base api service publishes a fixed host port
# (${API_PORT:-8000}:8000) which N>1 replicas cannot each bind. The
# docker-compose.scale.yml override empties the api `ports` so the service scales
# freely; clients reach the load-balanced API only through the dashboard.
# Override the replica count with `make scale N=5`.
N ?= 3
scale:
	docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --build \
		--scale api=$(N) api dashboard worker beat
	@echo "Scaled api to $(N) replicas behind the dashboard's nginx (Docker DNS round-robin)."
	@echo "Reach the load-balanced API via the dashboard: http://localhost:8080/api/health"
	@docker compose -f docker-compose.yml -f docker-compose.scale.yml ps

down:
	docker compose down

logs:
	docker compose logs -f api

# Apply migrations explicitly (the api entrypoint also does this on start). Runs a
# throwaway api container that overrides the entrypoint to invoke alembic directly.
migrate:
	docker compose run --rm --entrypoint alembic api upgrade head

# Generate a synthetic dataset and ingest it directly via the repository. Runs a
# throwaway api container; its entrypoint applies migrations first, so the schema
# is guaranteed present before the seed inserts. Override args, e.g.
#   make seed ARGS="--days 3 --metric response_time"
seed:
	docker compose run --rm api python -m scripts.seed $(ARGS)

# Rebuild the tester image first so a stale image can never mask a code change, then
# run the suite in a throwaway container.
test:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/unit -q

test-int:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/integration -q

# Full black-box end-to-end verifier (C13) in Docker against the LIVE api:
# bring up the api (built; postgres + redis come up as healthy deps), run the
# profile-gated `e2e` service (waits for the api healthcheck, then runs
# scripts/verify_e2e.py through the whole flow), capture its exit code, tear the
# stack down, and exit with that code so a failed check fails `make e2e`.
e2e:
	docker compose up -d --build api
	@status=0; \
	docker compose --profile e2e run --rm e2e || status=$$?; \
	docker compose down; \
	exit $$status

# Latency + throughput + memory gates (C13) in Docker against the LIVE api:
# bring up the api, run the profile-gated `loadtest` service (waits for the api
# healthcheck, then runs perf_test.py followed by load_test.py), capture its exit
# code, tear the stack down, and exit with that code so a failed gate fails `make load`.
# perf_test hard-gates the serve-path p95 (<2s); load_test hard-gates error-rate (~0)
# and a throughput floor, and reports RSS against a generous ceiling. Thresholds are
# env-tunable (PERF_MAX_SERVE_MS, LOAD_MIN_RPS, LOAD_MAX_RSS_MB, ...).
load:
	docker compose up -d --build api
	@status=0; \
	docker compose --profile loadtest run --rm loadtest || status=$$?; \
	docker compose down; \
	exit $$status

clean:
	docker compose down -v --remove-orphans
