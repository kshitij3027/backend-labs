# Multi-Framework Compliance Reporting Engine - Makefile
#
# All targets are Docker-first to match the repo convention: tests run
# inside the ``tester`` container, the service runs inside the ``app``
# container. Never run pytest directly on the host.
#
# Targets:
#   help          Print available targets (default).
#   build         Build all images defined in docker-compose.yml.
#   up            Start postgres + app services (detached).
#   down          Stop and remove the running stack.
#   logs          Tail logs from the ``app`` service.
#   test          Run the full pytest suite in the tester container.
#   test-unit     Run only ``tests/unit`` with verbose output.
#   test-int      Run only ``tests/integration`` with verbose output.
#   seed-logs     Seed ~5000 synthetic log events (script ships in a
#                 later commit).
#   seed-reports  POST a sample report per framework for demo purposes.
#   e2e           End-to-end probe inside tester container.
#   load          Load-test the live app from the tester container.
#   clean         Stop the stack, remove volumes, prune caches.

.PHONY: help build up down logs test test-unit test-int seed-logs seed-reports e2e load clean

help:
	@echo "Targets:"
	@echo "  build         - build all docker images"
	@echo "  up            - start postgres + app services (detached)"
	@echo "  down          - stop and remove the stack"
	@echo "  logs          - tail app logs"
	@echo "  test          - run the full test suite in Docker"
	@echo "  test-unit     - run only tests/unit with verbose output"
	@echo "  test-int      - run only tests/integration with verbose output"
	@echo "  seed-logs     - seed ~5000 synthetic log events (script ships in a later commit)"
	@echo "  seed-reports  - POST a sample report per framework"
	@echo "  e2e           - end-to-end probe inside tester container (requires 'make up' first)"
	@echo "  load          - load-test the live app from the tester container"
	@echo "  clean         - down + remove volumes + prune caches"

build:
	docker compose build

up:
	docker compose up -d postgres app

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm tester pytest -v

test-unit:
	docker compose --profile test run --rm tester pytest tests/unit -v

test-int:
	docker compose --profile test run --rm tester pytest tests/integration -v

seed-logs:
	docker compose --profile test run --rm tester python scripts/seed_logs.py --count 5000

seed-reports:
	docker compose --profile test run --rm tester python scripts/seed_reports.py

e2e:
	docker compose --profile test run --rm tester bash scripts/e2e.sh

load:
	docker compose --profile test run --rm tester python scripts/load_test.py

clean:
	docker compose down -v
	rm -rf .pytest_cache __pycache__
