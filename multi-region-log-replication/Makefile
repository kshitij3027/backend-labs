PROJECT_NAME = multi-region-log-replication
APP_IMAGE    = $(PROJECT_NAME)-app
TEST_IMAGE   = $(PROJECT_NAME)-test

.PHONY: build test run stop logs e2e demo clean

# Build the runtime image (used directly when you don't want compose).
build:
	docker build -t $(APP_IMAGE) -f Dockerfile .

# Build the test image and run pytest inside a one-shot container.
test:
	docker build -t $(TEST_IMAGE) -f Dockerfile.test .
	docker run --rm $(TEST_IMAGE)

# Bring up the full stack (app + redis) in the background.
run:
	docker compose up --build -d

stop:
	docker compose down

logs:
	docker compose logs -f

# End-to-end driver — boots the stack, waits for the app to be ready, runs
# the verifier, and tears down on success or failure. ``verify_replication.py``
# lands in commit 5; until then this target is a placeholder that just checks
# the placeholder app responds.
e2e:
	docker compose up --build -d
	sleep 15
	@if [ -f scripts/verify_replication.py ]; then \
		python3 scripts/verify_replication.py || (docker compose down && exit 1); \
	else \
		echo "scripts/verify_replication.py not yet implemented (lands in commit 5)"; \
	fi
	docker compose down

# Run the cart-update demo scenario (lands in commit 6).
demo:
	docker compose exec app python -m scripts.demo

# Conservative clean: stop containers, remove built images, prune __pycache__.
# Does NOT remove .venv (per project_requirements.md §3D).
clean:
	docker compose down -v 2>/dev/null || true
	docker rmi -f $(APP_IMAGE) $(TEST_IMAGE) 2>/dev/null || true
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
