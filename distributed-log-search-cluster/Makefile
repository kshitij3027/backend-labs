.PHONY: build up down test e2e benchmark logs clean

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

test:
	docker compose run --rm test pytest -v

e2e:
	docker compose run --rm test python scripts/e2e_smoke.py

benchmark:
	docker compose run --rm test python scripts/benchmark.py

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
