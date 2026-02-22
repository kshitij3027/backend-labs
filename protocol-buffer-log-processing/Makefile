.PHONY: build run test clean build-prod run-prod compare-images run-perf run-datagen run-high-load run-verify run-all-profiles

build:
	docker compose build app

run:
	docker compose run --rm app python main.py

test:
	docker compose build tests
	docker compose run --rm tests

clean:
	docker compose down --rmi local --volumes
	rm -rf logs/

build-prod:
	docker compose build app-production

run-prod:
	docker compose --profile production run --rm app-production python main.py

compare-images:
	@echo "Development image:"
	@docker images protocol-buffer-log-processing-app --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
	@echo ""
	@echo "Production image:"
	@docker images protocol-buffer-log-processing-app-production --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

# Complete compose targets
run-perf:
	docker compose -f docker-compose.complete.yml --profile perf run --rm perf

run-datagen:
	docker compose -f docker-compose.complete.yml --profile datagen run --rm datagen

run-high-load:
	docker compose -f docker-compose.complete.yml --profile high-load run --rm high-load

run-verify:
	docker compose -f docker-compose.complete.yml --profile verify run --rm verify

run-all-profiles:
	@echo "=== Running tests ==="
	docker compose -f docker-compose.complete.yml --profile test run --rm tests
	@echo "\n=== Running verification ==="
	docker compose -f docker-compose.complete.yml --profile verify run --rm verify
	@echo "\n=== Running normal pipeline ==="
	docker compose -f docker-compose.complete.yml run --rm app python main.py
	@echo "\nAll profiles completed successfully."
