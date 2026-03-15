.PHONY: build up down test e2e logs clean

build:
	docker compose build

up:
	docker compose up --build -d app

down:
	docker compose down

test:
	docker compose --profile test run --rm --build test

e2e:
	docker compose up --build -d app
	docker compose --profile e2e run --rm --build e2e
	docker compose down

logs:
	docker compose logs -f app

clean:
	docker compose down -v --rmi local
	docker image prune -f
