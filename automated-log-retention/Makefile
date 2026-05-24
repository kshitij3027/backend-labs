# Automated Log Retention - Makefile
#
# All targets are Docker-first to match the repo convention: tests run
# in the ``tester`` container, the service runs in the ``app`` container.
# Never run pytest on the host.
#
# Targets:
#   help        Print available targets (default).
#   build       Build all images defined in docker-compose.yml.
#   up          Start the ``app`` service in the background.
#   down        Stop and remove the running stack.
#   logs        Tail logs from the ``app`` service.
#   test        Run the full pytest suite in the tester container.
#   test-unit   Run only ``tests/unit`` with verbose output.
#   test-int    Run only ``tests/integration`` with verbose output.
#   e2e         Run scripts/e2e.sh inside the tester container against the live app (requires `make up` first).
#   throughput  Run scripts/throughput.py against the live app (requires `make up` first).
#   demo        Build + start; print dashboard URL.
#   clean       Stop the stack, remove volumes, prune local images.

.PHONY: help build up down logs test test-unit test-int e2e throughput demo clean

help:
	@echo "Targets:"
	@echo "  build       - build all docker images"
	@echo "  up          - start the app service (detached)"
	@echo "  down        - stop and remove the stack"
	@echo "  logs        - tail app logs"
	@echo "  test        - run the full test suite in Docker"
	@echo "  test-unit   - run only tests/unit with verbose output"
	@echo "  test-int    - run only tests/integration with verbose output"
	@echo "  e2e         - end-to-end probe inside tester container (requires 'make up' first)"
	@echo "  throughput  - throughput probe inside tester container (requires 'make up' first)"
	@echo "  demo        - build + start; open http://localhost:8000/"
	@echo "  clean       - down + remove locally-built images"

build:
	docker compose build

up:
	docker compose up -d app

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

# E2E probe runs scripts/e2e.sh from inside the ``tester`` compose
# service so the script talks to the app over the compose network DNS
# name (``http://app:8000``). The operator must bring the stack up first
# via ``make up`` — the target deliberately does NOT manage up/down so a
# flaky run never tears down a stack the operator wanted to keep
# inspecting. (scripts/e2e.sh is added in C19.)
e2e:
	docker compose --profile test run --rm -e BASE_URL=http://app:8000 tester bash scripts/e2e.sh

# Throughput probe runs scripts/throughput.py inside the ``tester``
# container so the request travels via the compose network
# (``http://app:8000``), matching the production wiring. Assumes the
# ``app`` service is already up via ``make up``. (scripts/throughput.py
# is added in C19.)
throughput:
	docker compose --profile test run --rm -e BASE_URL=http://app:8000 tester python scripts/throughput.py

demo:
	$(MAKE) build
	$(MAKE) up
	@echo "Open http://localhost:8000/"

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
