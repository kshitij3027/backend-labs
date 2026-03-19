.PHONY: build up down test e2e logs clean infra topic-check

PROJECT := exactly-once-kafka-transaction-processor

# Build all images
build:
	docker compose build

# Start full stack (infra + app)
up:
	docker compose up --build -d

# Stop everything and remove containers
down:
	docker compose down

# Run test suite inside Docker
test:
	docker compose --profile test up --build --abort-on-container-exit test

# Run end-to-end verification
e2e:
	docker compose up --build -d
	@echo "Waiting 30s for transactions to process..."
	@sleep 30
	docker compose exec app python scripts/verify_eos.py
	docker compose down

# Tail logs for all services
logs:
	docker compose logs -f

# Full cleanup: containers, volumes, images
clean:
	docker compose down -v --remove-orphans
	docker compose --profile test down -v --remove-orphans 2>/dev/null || true
	docker rmi $$(docker images -q --filter "reference=*$(PROJECT)*") 2>/dev/null || true

# Start only infrastructure (kafka brokers + postgres)
infra:
	docker compose up -d kafka1 kafka2 kafka3 postgres
	@echo "Waiting for brokers and postgres to become healthy..."
	docker compose up kafka-init db-init

# Verify topics exist on the cluster
topic-check:
	docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --list
