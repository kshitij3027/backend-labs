.PHONY: build up down test clean logs

build:
	docker compose build

up:
	docker compose up -d zookeeper kafka kafka-init

down:
	docker compose down -v

test:
	docker compose build test
	docker compose run --rm test

clean:
	docker compose down -v --rmi local
	docker system prune -f

logs:
	docker compose logs -f
