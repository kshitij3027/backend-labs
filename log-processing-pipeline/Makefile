.PHONY: build run stop clean query logs web web-stop metrics local-setup local-run local-stop

build:
	docker compose build

run:
	docker compose up -d

stop:
	docker compose down

clean:
	docker compose down -v

query:
	docker compose run --rm query $(ARGS)

logs:
	docker compose logs -f --tail=20

web:
	docker compose --profile web up -d

web-stop:
	docker compose --profile web down

metrics:
	@echo "--- Generator ---" && docker compose exec generator cat /logs/.generator_metrics.json 2>/dev/null || echo "(not available)"
	@echo "--- Collector ---" && docker compose exec collector cat /data/collected/.collector_metrics.json 2>/dev/null || echo "(not available)"
	@echo "--- Parser ---" && docker compose exec parser cat /data/parsed/.parser_metrics.json 2>/dev/null || echo "(not available)"
	@echo "--- Storage ---" && docker compose exec storage cat /data/storage/.storage_metrics.json 2>/dev/null || echo "(not available)"

local-setup:
	mkdir -p logs data/collected data/parsed data/storage/active data/storage/archive data/storage/index
	pip install -r requirements.txt

local-run:
	python pipeline.py start

local-stop:
	python pipeline.py stop
