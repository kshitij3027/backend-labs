.DEFAULT_GOAL := help
.PHONY: help build up up-all worker beat dashboard ui down logs migrate seed test test-unit test-int clean

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
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  clean      - down + remove volumes and orphans"
	@echo "  (e2e / load / ui targets are added in later commits)"

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

clean:
	docker compose down -v --remove-orphans
