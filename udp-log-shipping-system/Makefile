.PHONY: build run stop test clean logs

build:
	docker compose build

run:
	docker compose up -d udp-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

logs:
	docker compose logs -f udp-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/
