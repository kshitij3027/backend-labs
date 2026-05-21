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
#   throughput  Run scripts/throughput.py against the live `app` service (asserts >= 1000 logs/sec).
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
	@echo "  throughput  - throughput probe inside tester container (>=1000 logs/sec; requires 'make up' first)"
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

# E2E placeholder is wired in C12 (scripts/e2e.sh).
e2e:
	@echo "e2e - wired in C12"

# Throughput probe runs scripts/throughput.py inside the `tester`
# container so the request travels via the compose network
# (`http://app:8000`), matching the production wiring. Assumes the
# `app` service is already up via `make up` — the target intentionally
# does NOT bring the stack up/down so an operator can run repeated
# probes against the same warm service without paying restart cost.
.PHONY: throughput
throughput: ## Run the throughput probe inside the tester container against the live app (run `make up` first)
	docker compose --profile test run --rm -e BASE_URL=http://app:8000 tester python scripts/throughput.py

demo:
	$(MAKE) build
	$(MAKE) up
	@sleep 5
	@echo "Open http://localhost:8000/"

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
