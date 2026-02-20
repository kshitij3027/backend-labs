.PHONY: build run stop test load benchmark perf logs clean

build:
	docker compose build

run:
	docker compose up -d perf-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

load:
	docker compose --profile generator run --build --rm generator

benchmark:
	docker compose --profile benchmark run --build --rm benchmark

perf:
	docker compose -f docker-compose.yml -f docker-compose.perf.yml --profile generator run --build --rm generator

logs:
	docker compose logs -f perf-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/ certs/
