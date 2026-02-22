.PHONY: build run test clean build-prod run-prod compare-images

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
