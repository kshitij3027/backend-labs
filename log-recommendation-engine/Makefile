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
	@echo "  load       - latency + throughput + memory gates in Docker (from C19/C20)"
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

# Horizontal API scaling behind the dashboard's nginx (arrives in C23).
scale:
	@echo "scale: available from commit C23"

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

# Full black-box end-to-end verifier (arrives in C18).
e2e:
	@echo "e2e: available from commit C18"

# Latency + throughput + memory gates (arrive in C19/C20).
load:
	@echo "load: available from commit C19"

clean:
	docker compose down -v --remove-orphans
