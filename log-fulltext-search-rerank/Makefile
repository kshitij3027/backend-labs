.PHONY: build up down clean test e2e load demo logs ui

build:
	docker compose build
	docker compose --profile test build

up:
	./start.sh

down:
	docker compose down

clean:
	docker compose down -v --rmi local && docker image prune -f

test:
	docker compose --profile test run --rm test pytest -v --tb=short

e2e:
	./start.sh
	docker compose --profile test run --rm -e APP_URL=http://app:8000 test python scripts/demo.py
	docker compose down

load:
	./start.sh
	docker compose --profile test run --rm -e APP_URL=http://app:8000 -e CHECK_RSS=1 test python scripts/load_test.py; \
	rc=$$?; \
	docker compose down; \
	exit $$rc

demo:
	./start.sh
	docker compose --profile test run --rm -e APP_URL=http://app:8000 test python scripts/demo.py; \
	rc=$$?; \
	docker compose down; \
	exit $$rc

logs:
	docker compose logs -f app

ui:
	open http://localhost:8000/
