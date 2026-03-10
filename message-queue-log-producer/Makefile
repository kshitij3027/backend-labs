build:
	docker compose build

run:
	docker compose up -d

stop:
	docker compose down

test:
	docker compose run --rm test

e2e:
	docker compose run --rm app python scripts/verify_e2e.py

throughput:
	docker compose run --rm app python scripts/throughput_test.py

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f
