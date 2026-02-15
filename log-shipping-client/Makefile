.PHONY: build run stop test client batch logs clean

build:
	docker compose build

run:
	docker compose up -d log-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

client:
	docker compose --profile client up --build log-client

batch:
	docker compose --profile batch run --build --rm log-client-batch

logs:
	docker compose logs -f log-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/*.log
