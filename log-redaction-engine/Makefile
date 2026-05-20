# Intelligent Log Redaction Engine - Makefile
#
# All targets are Docker-first to match the repo convention: tests run in
# the ``tester`` container, the service runs in the ``app`` container.
# Never run pytest on the host.
#
# Targets:
#   help        Print available targets (default).
#   build       Build all images defined in docker-compose.yml.
#   up          Start the ``app`` service in the background.
#   down        Stop and remove the running stack.
#   logs        Tail logs from the ``app`` service.
#   test        Run the full pytest suite (with coverage) in the tester container.
#   test-unit   Run only ``tests/unit`` with verbose output.
#   e2e         Placeholder - wired in C12.
#   throughput  Placeholder - wired in C11.
#   demo        Build + start + print dashboard URL.
#   clean       Stop the stack, remove volumes, prune local images.

.PHONY: help build up down logs test test-unit e2e throughput demo clean

help:
	@echo "Targets:"
	@echo "  build       - build all docker images"
	@echo "  up          - start the app stack (detached)"
	@echo "  down        - stop and remove the stack"
	@echo "  logs        - tail app logs"
	@echo "  test        - run the full test suite in Docker"
	@echo "  test-unit   - run only tests/unit with verbose output"
	@echo "  e2e         - end-to-end probe (wired in C12)"
	@echo "  throughput  - throughput benchmark (wired in C11)"
	@echo "  demo        - build + start; open http://localhost:8000/"
	@echo "  clean       - down + remove locally-built images"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose --profile test run --rm tester pytest

test-unit:
	docker compose --profile test run --rm tester pytest tests/unit -v

# E2E and throughput probes are placeholders in C1. They are wired in the
# commits that introduce the scripts they call (C12 for ``scripts/e2e.sh``
# and C11 for ``scripts/throughput.py``).
e2e:
	@echo "e2e - wired in C12"

throughput:
	@echo "throughput - wired in C11"

demo:
	$(MAKE) build
	$(MAKE) up
	@sleep 5
	@echo "Open http://localhost:8000/"

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
