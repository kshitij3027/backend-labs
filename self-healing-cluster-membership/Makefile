.PHONY: build test run stop logs e2e clean

PROJECT_NAME = self-healing-cluster
TEST_IMAGE = $(PROJECT_NAME)-test
NODE_IMAGE = $(PROJECT_NAME)-node

build:
	docker build -t $(NODE_IMAGE) -f Dockerfile .

test:
	docker build -t $(TEST_IMAGE) -f Dockerfile.test .
	docker run --rm $(TEST_IMAGE)

run:
	docker compose up --build -d

stop:
	docker compose down

logs:
	docker compose logs -f

e2e:
	docker compose up --build -d
	sleep 8
	PYTHONPATH=. python3 scripts/verify_cluster.py || (docker compose down && exit 1)
	docker compose down

clean:
	docker compose down 2>/dev/null || true
	docker rmi -f $(TEST_IMAGE) $(NODE_IMAGE) 2>/dev/null || true
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
