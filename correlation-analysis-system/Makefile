.DEFAULT_GOAL := help
.PHONY: help build up down logs test test-unit test-int clean

help:
	@echo "Correlation Analysis System — make targets:"
	@echo "  build      - build all images (backend + test)"
	@echo "  up         - run the runtime (backend, detached, with redis)"
	@echo "  down       - stop and remove the stack"
	@echo "  logs       - tail the backend logs"
	@echo "  test       - full pytest suite in Docker (rebuilds first)"
	@echo "  test-unit  - unit tests only in Docker"
	@echo "  test-int   - integration tests only in Docker"
	@echo "  clean      - down + remove volumes and orphans"

build:
	docker compose build

# Bring up the backend detached (redis comes up as a healthy dep).
up:
	docker compose up -d --build backend
	@echo "API: http://localhost:$${BACKEND_PORT:-8000}  (GET /health)"

down:
	docker compose down

logs:
	docker compose logs -f backend

# Rebuild the tester image first so a stale image can never mask a code change, then
# run the suite in a throwaway container. Exit status propagates so a failed test
# fails `make test`; the stack is torn down afterwards regardless.
test:
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest -q || status=$$?; \
	docker compose down -v; \
	exit $$status

test-unit:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/unit -q || status=$$?; \
	docker compose down; \
	exit $$status

test-int:
	docker compose build test
	@status=0; \
	docker compose --profile test run --rm test pytest tests/integration -q || status=$$?; \
	docker compose down; \
	exit $$status

# Placeholder targets (lands in C8/C9):
# ui:    - bring up redis + backend + frontend dashboard, print the URL (C9)
# e2e:   - black-box end-to-end verifier in Docker via scripts/verify_e2e.py (C8)
# load:  - perf + concurrent load gates in Docker via scripts/load_test.py (C8)

clean:
	docker compose down -v --remove-orphans
