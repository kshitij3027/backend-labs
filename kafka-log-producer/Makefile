.PHONY: build up down test clean logs e2e benchmark

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

e2e:
	docker compose --profile e2e up --build --abort-on-container-exit e2e

benchmark:
	docker compose --profile benchmark up --build --abort-on-container-exit benchmark
