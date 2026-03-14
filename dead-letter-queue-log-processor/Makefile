.PHONY: build up down test e2e logs clean

build:
	docker compose build

up:
	docker compose up --build -d redis app

down:
	docker compose down -v

test:
	docker compose run --rm --build test

e2e:
	docker compose --profile e2e up --build --abort-on-container-exit

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
