.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int e2e load ui clean

help:
	@echo "ML Log Classifier — make targets:"
	@echo "  build      - build the app + test images"
	@echo "  up         - run the app live on http://localhost:8000 (detached)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the app logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  e2e        - end-to-end verifier in Docker (added in a later commit)"
	@echo "  load       - load / performance test in Docker (added in a later commit)"
	@echo "  ui         - run app + React dashboard on http://localhost:8080 (detached)"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

up:
	docker compose up -d --build app
	@echo "API: http://localhost:8000"

down:
	docker compose down

logs:
	docker compose logs -f app

# Rebuild the tester image first so a stale image can never mask a code change,
# then run the full suite in a throwaway container.
test:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/unit -q

test-int:
	docker compose --profile test build
	docker compose --profile test run --rm test pytest tests/integration -q

# Placeholders — the e2e / load flows are wired up in later commits.
e2e:
	@echo "e2e added in a later commit"

load:
	@echo "load added in a later commit"

# Bring up the backend + the nginx-served React dashboard. The dashboard waits
# for the app healthcheck before starting (see docker-compose.yml).
ui:
	docker compose up -d --build app dashboard
	@echo "Dashboard: http://localhost:8080"
	@echo "API:       http://localhost:8000"

clean:
	docker compose down -v --remove-orphans
