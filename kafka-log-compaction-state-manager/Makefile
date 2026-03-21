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
	docker compose --profile benchmark up --build --abort-on-container-exit

logs:
	docker compose logs -f

clean:
	docker compose down -v --remove-orphans
	docker system prune -f
