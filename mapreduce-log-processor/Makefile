.PHONY: build run stop test e2e logs clean

build:
	docker compose build

run:
	docker compose up --build -d

stop:
	docker compose down

test:
	docker compose --profile test up --build --abort-on-container-exit test

e2e:
	@echo "--- Tearing down any existing services ---"
	docker compose down -v
	@echo "--- Starting services ---"
	docker compose up --build -d
	@echo "--- Waiting for coordinator to be healthy ---"
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		if curl -sf http://localhost:8000/health > /dev/null 2>&1; then \
			echo "Coordinator is healthy"; \
			break; \
		fi; \
		echo "Waiting... ($$i)"; \
		sleep 3; \
	done
	@echo "--- Running E2E verification ---"
	docker compose run --rm e2e
	@echo "--- Tearing down ---"
	docker compose down -v

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local --remove-orphans
