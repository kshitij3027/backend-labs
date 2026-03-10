build:
	docker compose build

run:
	docker compose up -d

stop:
	docker compose down

test:
	docker compose run --rm test

e2e:
	docker compose up -d rabbitmq app
	docker compose run --rm e2e
	docker compose down

throughput:
	docker compose up -d rabbitmq app
	docker compose run --rm e2e python scripts/throughput_test.py
	docker compose down

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f
