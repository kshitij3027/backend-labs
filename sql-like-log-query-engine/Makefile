.PHONY: build up down test e2e demo load ui logs clean

build:
	docker compose build

up:
	docker compose up -d partition-1 partition-2 partition-3 coordinator

down:
	docker compose down -v

test:
	docker compose run --rm test pytest -v

e2e:
	./start.sh && docker compose run --rm test python scripts/demo.py && ./stop.sh

demo:
	docker compose run --rm test python scripts/demo.py

load:
	docker compose run --rm test python scripts/load_test.py --concurrent-users 20 --duration 30 --target-url http://coordinator:8000

ui:
	@command -v open >/dev/null 2>&1 && open http://localhost:8000/ \
		|| command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000/ \
		|| echo "Open http://localhost:8000"

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
