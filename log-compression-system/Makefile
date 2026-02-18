.PHONY: build run stop test client logs clean benchmark

build:
	docker compose build

run:
	docker compose up -d log-receiver
	@echo "Server running on TCP port $${SERVER_PORT:-5000}"

stop:
	docker compose down

test:
	docker compose build tests
	docker compose run --rm tests

client:
	docker compose run --rm log-shipper

logs:
	docker compose logs -f log-receiver

benchmark:
	docker compose run --rm tests python benchmark.py

clean:
	docker compose down -v --rmi local
	docker image prune -f
