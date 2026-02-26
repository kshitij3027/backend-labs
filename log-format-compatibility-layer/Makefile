.PHONY: build test run clean

build:
	docker compose build

test:
	docker compose build test
	docker compose run --rm test

run:
	docker compose up app

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
