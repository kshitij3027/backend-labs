# Field-Level Log Encryption Service — Makefile
#
# All targets are Docker-first to match the repo convention: tests run in the
# `tester` container, the service runs in the `app` container. Never run
# pytest on the host.
#
# Targets:
#   build       Build all images defined in docker-compose.yml.
#   up          Start the `app` service in the background.
#   down        Stop and remove the running stack (preserves named volumes).
#   logs        Tail logs from the `app` service.
#   test        Run the full pytest suite (including coverage) in the tester container.
#   test-unit   Run only tests not marked `integration`.
#   clean       Stop the stack, remove volumes, and prune locally-built images.

.PHONY: build up down logs test test-unit clean help

help:
	@echo "Targets:"
	@echo "  build       - build all docker images"
	@echo "  up          - start the app service (detached)"
	@echo "  down        - stop and remove the stack"
	@echo "  logs        - tail app logs"
	@echo "  test        - run the full test suite in Docker"
	@echo "  test-unit   - run unit tests only (excludes integration)"
	@echo "  clean       - down + remove locally-built images"

build:
	docker compose --profile test build

up:
	docker compose up -d app

down:
	docker compose down -v

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --build --rm tester

test-unit:
	docker compose --profile test run --rm tester pytest -v -k "not integration"

clean:
	docker compose down -v --rmi local
