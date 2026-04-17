.PHONY: build up down test e2e demo load ui logs clean

build:
	docker compose build

up:
	docker compose up -d partition-1 partition-2 partition-3 coordinator

down:
	docker compose down -v

test:
	docker compose run --rm test pytest -v

e2e:
	docker compose run --rm test python scripts/demo.py

demo: e2e

load:
	docker compose run --rm test python scripts/load_test.py --concurrent-users 20 --duration 30 --target-url http://coordinator:8000

ui:
	@echo "UI at http://localhost:8000 once coordinator is up"

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
