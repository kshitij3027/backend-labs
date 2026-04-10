.PHONY: build up down logs test e2e load clean

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm test pytest -v

e2e:
	docker compose --profile e2e up --build --abort-on-container-exit

load:
	docker compose --profile loadtest up --build --abort-on-container-exit

clean:
	docker compose down -v
