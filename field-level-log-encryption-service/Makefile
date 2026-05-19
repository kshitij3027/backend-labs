# Field-Level Log Encryption Service — Makefile
#
# All targets are Docker-first to match the repo convention: tests run in the
# `tester` container, the service runs in the `app` container. Never run
# pytest on the host.
#
# Targets:
#   build       Build all images defined in docker-compose.yml.
#   up          Start the `app` service (and its `redis` dep) in the background.
#   down        Stop and remove the running stack (preserves named volumes).
#   logs        Tail logs from the `app` service.
#   test        Run the full pytest suite (including coverage) in the tester container.
#   test-unit   Run only tests not marked `integration`.
#   e2e         Bring stack up, run scripts/e2e.sh against it, tear down.
#   throughput  Bring stack up, run scripts/throughput.py (≥50 logs/sec), tear down.
#   demo        Build, start the stack, and print the dashboard URL.
#   clean       Stop the stack, remove volumes, and prune locally-built images.

.PHONY: build up down logs test test-unit e2e throughput demo clean help

help:
	@echo "Targets:"
	@echo "  build       - build all docker images"
	@echo "  up          - start the app + redis stack (detached)"
	@echo "  down        - stop and remove the stack"
	@echo "  logs        - tail app logs"
	@echo "  test        - run the full test suite in Docker"
	@echo "  test-unit   - run unit tests only (excludes integration)"
	@echo "  e2e         - run scripts/e2e.sh against a live stack (auto up/down)"
	@echo "  throughput  - run scripts/throughput.py — assert >=50 logs/sec"
	@echo "  demo        - build + start; open http://localhost:8000/ in a browser"
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

# ---------------------------------------------------------------------------
# E2E: scripts/e2e.sh against a live stack
# ---------------------------------------------------------------------------
#
# Bring the stack up, give the app's healthcheck a moment to flip green,
# run the E2E script, capture its exit code, then ALWAYS tear the stack
# down — even on failure — so a broken E2E never leaves a stale stack
# eating port 8000. The captured exit code is returned to the caller.
e2e: up
	@echo "[make e2e] waiting for app healthcheck..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  if curl -fsS http://localhost:8000/api/health >/dev/null 2>&1; then \
	    echo "[make e2e] app healthy"; break; \
	  fi; \
	  echo "[make e2e] not ready yet (attempt $$i/10), sleeping 2s"; \
	  sleep 2; \
	done
	@set +e; bash scripts/e2e.sh; ec=$$?; \
	  $(MAKE) down; \
	  exit $$ec

# ---------------------------------------------------------------------------
# Throughput: scripts/throughput.py against a live stack
# ---------------------------------------------------------------------------
#
# Runs the throughput probe from inside the `tester` compose service so
# the request travels via the compose network (`http://app:8000`),
# matching the production wiring. Always tears down on exit so a flaky
# host pythons/jq install can never block the next run.
throughput: up
	@echo "[make throughput] waiting for app healthcheck..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  if curl -fsS http://localhost:8000/api/health >/dev/null 2>&1; then \
	    echo "[make throughput] app healthy"; break; \
	  fi; \
	  echo "[make throughput] not ready yet (attempt $$i/10), sleeping 2s"; \
	  sleep 2; \
	done
	@set +e; \
	  docker compose --profile test run --rm \
	    -e BASE_URL=http://app:8000 \
	    tester python scripts/throughput.py http://app:8000; \
	  ec=$$?; \
	  $(MAKE) down; \
	  exit $$ec

# ---------------------------------------------------------------------------
# Demo: build + start; print where to open the dashboard
# ---------------------------------------------------------------------------
#
# Convenience for first-time demos. We do `make build` so the operator
# doesn't get a stale image on a fresh clone, then `make up` and a
# short heredoc telling them where to point their browser. Leaves the
# stack running — `make down` to stop.
demo: build up
	@echo "[make demo] waiting for app healthcheck..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  if curl -fsS http://localhost:8000/api/health >/dev/null 2>&1; then \
	    echo "[make demo] app healthy"; break; \
	  fi; \
	  echo "[make demo] not ready yet (attempt $$i/10), sleeping 2s"; \
	  sleep 2; \
	done
	@echo ""
	@echo "============================================================"
	@echo "  Field-Level Log Encryption Service is up."
	@echo ""
	@echo "  Dashboard:    http://localhost:8000/"
	@echo "  API base:     http://localhost:8000/v1/"
	@echo "  Health:       http://localhost:8000/api/health"
	@echo "  Metrics:      http://localhost:8000/metrics"
	@echo ""
	@echo "  When done:    make down"
	@echo "============================================================"

clean:
	docker compose down -v --rmi local
