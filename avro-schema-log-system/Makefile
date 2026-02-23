.PHONY: build run test clean

build:
	docker compose build

run:
	docker compose up -d app

test:
	docker compose build tests
	docker compose run --rm tests

clean:
	docker compose down -v --rmi local
	docker system prune -f
