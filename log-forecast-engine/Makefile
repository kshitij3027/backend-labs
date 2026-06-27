.DEFAULT_GOAL := help
.PHONY: help build up down logs migrate seed test test-unit test-int clean

help:
	@echo "Predictive Log Analytics Engine — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the API live on http://localhost:8000 (detached, with postgres + redis)"
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

up:
	docker compose up -d --build api
	@echo "API: http://localhost:8000"

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
