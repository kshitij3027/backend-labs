.PHONY: build up down clean test e2e load demo logs ui

build:
	docker compose build
	docker compose --profile test build

up:
	./start.sh

down:
	docker compose down

clean:
	docker compose down -v --rmi local && docker image prune -f

test:
	docker compose --profile test run --rm test pytest -v --tb=short

e2e:
	@echo "e2e tests wired in commit 04+"; exit 0

load:
	@echo "load tests wired in commit 10"; exit 0

demo:
	@echo "demo wired in commit 10"; exit 0

logs:
	docker compose logs -f app

ui:
	open http://localhost:8000/
