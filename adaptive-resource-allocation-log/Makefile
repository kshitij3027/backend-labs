.DEFAULT_GOAL := help
.PHONY: help test test-unit

help:
	@echo "Adaptive Resource Allocation — make targets:"
	@echo "  test       - run the full pytest suite in Docker"
	@echo "  test-unit  - run only tests/unit in Docker"

test:
	docker compose --profile test run --rm test pytest -q

test-unit:
	docker compose --profile test run --rm test pytest tests/unit -q
