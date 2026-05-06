PROJECT := log-service-circuit-breaker-engine

.PHONY: build test run stop logs clean e2e demo

build:
	docker compose build

test:
	docker compose --profile test up --build --abort-on-container-exit test; docker compose --profile test down

run:
	docker compose up --build -d

stop:
	docker compose down

logs:
	docker compose logs -f

clean:
	docker compose down -v --remove-orphans; docker image prune -f

e2e:
	@echo "E2E will be added in Commit 17"

demo:
	@echo "Demo will be added in Commit 13"
