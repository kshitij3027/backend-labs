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

seed:
	docker compose run --rm tester python -m scripts.seed_data

demo:
	docker compose run --rm tester bash scripts/demo.sh

load-test:
	docker compose run --rm tester python -m scripts.load_test
