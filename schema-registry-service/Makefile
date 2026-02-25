.PHONY: build run test e2e clean

build:
	docker compose build

run:
	docker compose up -d app

test:
	docker compose build tests
	docker compose run --rm tests

e2e:
	@echo "E2E tests not yet implemented"

clean:
	docker compose down -v --rmi local
	docker system prune -f
