.PHONY: build up down clean test e2e load demo logs ui

build:
	docker compose build

up:
	./start.sh

down:
	docker compose down

clean:
	docker compose down -v && docker image prune -f

test:
	docker compose --profile test run --rm test pytest -v --tb=short --ignore=tests/test_e2e.py

e2e:
	./start.sh && docker compose --profile test run --rm test pytest -v tests/test_e2e.py; code=$$?; docker compose down; exit $$code

load:
	./start.sh && docker compose --profile test run --rm test python scripts/load_test.py

demo:
	./start.sh && docker compose --profile test run --rm test python scripts/demo.py

logs:
	docker compose logs -f app

ui:
	open http://localhost:8080/
