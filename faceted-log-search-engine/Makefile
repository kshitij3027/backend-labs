.PHONY: build up down clean test e2e demo load logs ui

build:
	docker compose build

up:
	./start.sh

down:
	docker compose down

clean:
	docker compose down -v --rmi local

test:
	docker compose --profile test run --rm test pytest -v

e2e:
	./start.sh && docker compose --profile test run --rm test pytest -v tests/test_e2e.py; ./stop.sh

demo:
	docker compose --profile test run --rm test python scripts/demo.py

load:
	docker compose --profile test run --rm test python scripts/load_test.py

logs:
	docker compose logs -f app

ui:
	@command -v open >/dev/null 2>&1 && open http://localhost:8000 \
		|| command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000 \
		|| echo "Open http://localhost:8000"
