PROJECT := adaptive-backpressure-manager

.PHONY: build test run stop logs clean e2e dashboard loadtest demo

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
		hs=$$(docker inspect --format '{{.State.Health.Status}}' abpm-app 2>/dev/null || echo starting); \
		if [ "$$hs" = "healthy" ]; then echo "app healthy"; break; fi; \
		sleep 2; \
	done
	docker compose --profile test run --rm test python -m scripts.verify_e2e --base http://app:8000
	docker compose down

dashboard:
	@echo "Dashboard available at http://localhost:8050"
	docker compose up -d app dashboard

loadtest:
	@curl -fsS -X POST http://localhost:8000/api/v1/loadtest/start \
	  -H 'Content-Type: application/json' \
	  -d '{"profile":"full","rps":200,"duration_seconds":60,"spike_multiplier":10.0}' | jq .

demo:
	docker compose up -d app dashboard
	@for i in $$(seq 1 30); do \
	  hs=$$(docker inspect --format '{{.State.Health.Status}}' abpm-app 2>/dev/null || echo starting); \
	  if [ "$$hs" = "healthy" ]; then echo "app healthy"; break; fi; \
	  sleep 2; \
	done
	@echo "Driving a 30s smoke + spike. Watch http://localhost:8050"
	@curl -fsS -X POST http://localhost:8000/api/v1/loadtest/start \
	  -H 'Content-Type: application/json' \
	  -d '{"profile":"full","rps":200,"duration_seconds":60,"spike_multiplier":10.0}' >/dev/null
	docker compose logs -f app
