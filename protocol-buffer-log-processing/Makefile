.PHONY: build run test clean

build:
	docker compose build app

run:
	docker compose run --rm app python main.py

test:
	docker compose build tests
	docker compose run --rm tests

clean:
	docker compose down --rmi local --volumes
	rm -rf logs/
