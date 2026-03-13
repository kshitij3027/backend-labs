.PHONY: build run stop test e2e clean logs dashboard throughput demo sim

build:
	docker compose build

run:
	docker compose up -d rabbitmq

stop:
	docker compose down

test:
	docker compose run --rm test

e2e:
	docker compose run --rm e2e

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f

dashboard:
	docker compose up -d dashboard

throughput:
	docker compose run --rm app python scripts/throughput_test.py

demo:
	docker compose run --rm app python scripts/run_demo.py

sim:
	docker compose run --rm app python scripts/multi_service_sim.py
