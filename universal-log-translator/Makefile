.PHONY: build test verify clean

build:
	docker compose build

test:
	docker compose build tests
	docker compose run --rm tests

verify:
	docker compose build verify
	docker compose run --rm verify

clean:
	docker compose down -v --rmi local
	docker system prune -f
