SHELL := /bin/bash

.PHONY: help build up down logs ps test e2e load clean rebuild

help:
	@echo "build    - Build all images via docker compose"
	@echo "up       - Start the chaos-framework stack in detached mode"
	@echo "down     - Stop the stack"
	@echo "logs     - Tail logs for the chaos-framework service"
	@echo "ps       - Show running compose services"
	@echo "test     - Run unit tests inside Docker (wired up in C2+)"
	@echo "e2e      - Run end-to-end scenarios inside Docker (wired up in C7+)"
	@echo "load     - Run load / overhead tests inside Docker (wired up in C18)"
	@echo "clean    - Tear down volumes and remove local SQLite / cache artifacts"
	@echo "rebuild  - down + build + up"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=200 chaos-framework

ps:
	docker compose ps

test:
	docker compose --profile test run --rm test pytest tests/unit -v

e2e:
	@echo "e2e target wired up in C7+"
	@exit 0

load:
	@echo "load target wired up in C18"
	@exit 0

clean:
	docker compose down -v --remove-orphans
	rm -rf data/*.db data/*.sqlite* htmlcov .pytest_cache .ruff_cache 2>/dev/null || true

rebuild:
	$(MAKE) down
	$(MAKE) build
	$(MAKE) up
