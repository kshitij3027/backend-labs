.PHONY: help build build-test up down logs test test-unit test-integration e2e ui-e2e clean

help:
	@echo "Targets:"
	@echo "  build              - build the backend image"
	@echo "  build-test         - build the tester image"
	@echo "  up                 - start the stack (backend) detached"
	@echo "  down               - stop and remove the stack"
	@echo "  logs               - tail logs from all services"
	@echo "  test               - run full pytest suite in Docker (unit + integration)"
	@echo "  test-unit          - run only unit tests"
	@echo "  test-integration   - run only integration tests"
	@echo "  e2e                - run end-to-end script (filled in C14)"
	@echo "  ui-e2e             - reminder: Chrome MCP walkthrough is main-thread only"
	@echo "  clean              - stop stack, remove volumes, prune images"

build:
	docker compose build backend

build-test:
	docker compose build tester

up:
	docker compose up -d --wait backend

down:
	docker compose down

logs:
	docker compose logs -f

test: build-test
	docker compose run --rm tester pytest -v

test-unit: build-test
	docker compose run --rm tester pytest tests/unit -v

test-integration: build-test
	docker compose run --rm tester pytest tests/integration -v

e2e:
	@echo "[TODO] scripts/e2e.sh (filled in C14)"

ui-e2e:
	@echo "Run the Chrome MCP UI walkthrough from the main Claude thread (see plan.md Final Verification step 6)."

clean:
	docker compose down -v
	docker image prune -f
