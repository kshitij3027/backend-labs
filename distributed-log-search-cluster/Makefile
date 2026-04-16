.PHONY: build up down test e2e distribution benchmark failure logs clean

build:
	docker compose build

up:
	docker compose up -d redis node-1 node-2 node-3 node-4 coordinator

down:
	docker compose down -v

test:
	docker compose run --rm test pytest -v

e2e:
	docker compose run --rm test python scripts/e2e_smoke.py

distribution:
	docker compose run --rm test python scripts/distribution_test.py

benchmark:
	docker compose run --rm test python scripts/benchmark.py

failure:
	docker compose run --rm test python scripts/failure_test.py

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
