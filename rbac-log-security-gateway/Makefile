.PHONY: help build up down logs test test-unit test-integration e2e ui-e2e clean

help:
	@echo "Targets:"
	@echo "  build              - docker compose build all services"
	@echo "  up                 - docker compose up -d --wait"
	@echo "  down               - docker compose down"
	@echo "  logs               - tail logs from all services"
	@echo "  test               - run full pytest suite in Docker"
	@echo "  test-unit          - run unit tests in Docker"
	@echo "  test-integration   - run integration tests in Docker"
	@echo "  e2e                - run end-to-end test script"
	@echo "  ui-e2e             - reminder to run Chrome MCP walkthrough"
	@echo "  clean              - docker compose down -v + image prune"

build:
	@echo "[TODO] docker compose build (filled in C2)"

up:
	@echo "[TODO] docker compose up -d --wait (filled in C2)"

down:
	@echo "[TODO] docker compose down (filled in C2)"

logs:
	@echo "[TODO] docker compose logs -f (filled in C2)"

test:
	@echo "[TODO] docker compose run --rm tester pytest -v (filled in C2/C3)"

test-unit:
	@echo "[TODO] docker compose run --rm tester pytest tests/unit -v (filled in C2/C3)"

test-integration:
	@echo "[TODO] docker compose run --rm tester pytest tests/integration -v (filled in C2/C3)"

e2e:
	@echo "[TODO] scripts/e2e.sh (filled in C14)"

ui-e2e:
	@echo "Run the Chrome MCP UI walkthrough from the main Claude thread (see plan.md Final Verification step 6)."

clean:
	@echo "[TODO] docker compose down -v && docker image prune -f (filled in C2)"
