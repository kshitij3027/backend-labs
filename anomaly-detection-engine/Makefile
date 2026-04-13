.PHONY: build up down logs test e2e load clean restart

build:
	docker compose build

up:
	docker compose up --build -d

down:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f engine

test:
	docker compose --profile test run --rm --build test

e2e:
	docker compose up --build -d engine && sleep 15 && docker compose --profile e2e run --rm e2e; docker compose down -v --remove-orphans

load:
	docker compose --profile loadtest run --rm --build loadtest

clean:
	docker compose down -v --remove-orphans && docker rmi -f $$(docker images -q anomaly-detection-engine*) 2>/dev/null || true

restart: down up
