.PHONY: build up down infra-up infra-down test e2e logs clean

build:
	docker compose build

up:
	docker compose up --build -d

down:
	docker compose down

infra-up:
	docker compose up -d zookeeper kafka-1 kafka-2 kafka-3 kafka-init kafka-ui

infra-down:
	docker compose down

test:
	docker compose --profile test run --rm --build test

e2e:
	docker compose up --build -d
	docker compose --profile e2e run --rm --build e2e

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
	docker image prune -f
