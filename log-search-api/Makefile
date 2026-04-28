.PHONY: build build-test up down down-v logs test test-unit test-integration lint seed demo load-test

build:
	docker compose build api

build-test:
	docker compose build tester

up:
	docker compose up -d --wait

down:
	docker compose down

down-v:
	docker compose down -v

logs:
	docker compose logs -f api

test:
	docker compose run --rm tester pytest -v

test-unit:
	docker compose run --rm tester pytest tests/unit -v

test-integration:
	docker compose run --rm tester pytest tests/integration -v

lint:
	docker compose run --rm tester python -m compileall -q src tests

# Seed ~5,000 synthetic log entries via the live API. Runs in the api
# container so it can reach api:8000 over the compose network and reuses
# the project's existing Python image (with httpx installed).
seed:
	docker compose run --rm --no-deps api python scripts/seed_data.py

# End-to-end demo: spin up the stack, seed sample data, run ten example
# searches, and print the /stats snapshot. Leaves the stack running.
demo:
	bash scripts/demo.sh

# Full 500-request load test (uncached + cached phases). Asserts
# p95_uncached < 500ms and p95_cached < 100ms; non-zero exit on breach.
load-test:
	docker compose run --rm --no-deps api python scripts/load_test.py
