.DEFAULT_GOAL := help
.PHONY: help build up down logs migrate seed ui scale test test-unit test-int e2e load clean

help:
	@echo "Log Recommendation Engine — make targets:"
	@echo "  build      - build all images (api + test)"
	@echo "  up         - run the runtime (api, detached, with postgres + redis)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the api logs"
	@echo "  migrate    - apply Alembic migrations against Postgres in Docker (no-op until C2)"
	@echo "  seed       - generate + ingest a synthetic incident corpus in Docker (from C3)"
	@echo "  ui         - bring up api + dashboard (from C15)"
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

# Generate a synthetic incident corpus and ingest it (arrives in C3). Runs a
# throwaway api container. Override args, e.g. `make seed ARGS="--count 200"`.
seed:
	@echo "seed: available from commit C3"

# Bring up the API + dashboard together (dashboard arrives in C15).
ui:
	@echo "ui: available from commit C15"

# Horizontal API scaling behind the dashboard's nginx (arrives in C23).
scale:
	@echo "scale: available from commit C23"

# Rebuild the tester image first so a stale image can never mask a code change, then
# run the suite in a throwaway container. Exit status propagates so a failed test
# fails `make test`; the stack is torn down afterwards regardless.
test:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest -q || status=$$?; \
	docker compose down; \
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
