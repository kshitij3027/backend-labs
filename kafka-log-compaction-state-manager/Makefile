.PHONY: build up down test e2e benchmark logs clean

build:
	docker compose build

up:
	docker compose up --build -d zookeeper kafka kafka-init app

down:
	docker compose down -v

test:
	docker compose run --rm --build test

e2e:
	docker compose up --build -d zookeeper kafka kafka-init app
	docker compose --profile e2e run --rm --build e2e

benchmark:
	docker compose up --build -d zookeeper kafka kafka-init
	@echo "Waiting for Kafka to be ready..."
	@sleep 30
	docker compose run --rm --build benchmark
	docker compose down -v

logs:
	docker compose logs -f

clean:
	docker compose down -v --remove-orphans
	docker system prune -f
