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
	docker compose up -d --build
	@for i in $$(seq 1 30); do \
		hs=$$(docker inspect --format '{{.State.Health.Status}}' cb-engine-app 2>/dev/null || echo starting); \
		if [ "$$hs" = "healthy" ]; then echo "app healthy"; break; fi; \
		sleep 2; \
	done
	docker compose --profile test run --rm test python -m scripts.verify_e2e --base http://app:8000
	docker compose down

demo:
	docker compose run --rm app python -m src.demo --logs 15 --simulate-duration 2 --no-wait-recovery
