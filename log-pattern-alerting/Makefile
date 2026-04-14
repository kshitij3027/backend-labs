.PHONY: build up down test e2e loadtest logs init

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

test:
	docker compose --profile test run --rm test

e2e:
	docker compose --profile e2e run --rm e2e

loadtest:
	docker compose --profile loadtest run --rm loadtest

logs:
	docker compose logs -f app

init:
	docker compose run --rm db-init
