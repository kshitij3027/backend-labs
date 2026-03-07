.PHONY: build test run stop logs e2e seed clean

build:
	docker compose build

test: build
	docker compose run --rm test

run: build
	docker compose up -d coordinator

stop:
	docker compose down

logs:
	docker compose logs -f

e2e: build
	docker compose up -d coordinator
	@sleep 8
	python3 scripts/verify_e2e.py
	docker compose down

seed:
	python3 scripts/seed_data.py

clean:
	docker compose down -v --rmi local
