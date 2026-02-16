.PHONY: build run stop test clean logs client loadtest

build:
	docker compose build

run:
	docker compose up -d udp-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

client:
	docker compose --profile client run --build --rm udp-client

loadtest:
	docker compose --profile loadtest run --build --rm loadtest

logs:
	docker compose logs -f udp-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/
