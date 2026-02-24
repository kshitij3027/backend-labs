.PHONY: build test clean

build:
	docker compose build

test:
	docker compose build tests
	docker compose run --rm tests

clean:
	docker compose down -v --rmi local
	docker system prune -f
