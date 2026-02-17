.PHONY: build run stop test client logs clean

build:
	docker compose build

run:
	docker compose up -d log-server
	@echo "Server running on UDP port $${SERVER_PORT:-9999}"

stop:
	docker compose down

test:
	docker compose build tests
	docker compose run --rm tests

client:
	docker compose run --rm log-client

logs:
	docker compose logs -f log-server

clean:
	docker compose down -v --rmi local
	docker image prune -f
