.PHONY: build up down test e2e loadtest clean logs

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

test:
	docker compose build test
	docker compose run --rm test

e2e:
	docker compose --profile e2e up --build --abort-on-container-exit e2e

loadtest:
	docker compose --profile loadtest up --build --abort-on-container-exit loadtest

clean:
	docker compose down -v --rmi local
	docker system prune -f

logs:
	docker compose logs -f
