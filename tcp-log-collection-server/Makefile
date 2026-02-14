.PHONY: build run stop test clean client logs

build:
	docker compose build

run:
	docker compose up -d tcp-log-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

client:
	docker compose --profile client run --build --rm client

logs:
	docker compose logs -f tcp-log-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/
