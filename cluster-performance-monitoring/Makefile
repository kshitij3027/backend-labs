.PHONY: build test run stop e2e clean logs load-test

build:
	docker compose build

test:
	docker compose run --rm test

run:
	docker compose up -d monitor

stop:
	docker compose down

e2e:
	docker compose up -d monitor
	@python3 scripts/verify_e2e.py; EXIT_CODE=$$?; docker compose down; exit $$EXIT_CODE

load-test:
	docker compose run --rm test python scripts/load_test.py

clean:
	docker compose down -v --rmi local
	rm -rf data/ __pycache__ .pytest_cache

logs:
	docker compose logs -f monitor
