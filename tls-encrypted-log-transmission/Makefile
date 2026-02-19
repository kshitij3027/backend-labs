.PHONY: build run stop test client healthcare verified logs clean

build:
	docker compose build

run:
	docker compose up -d tls-server

stop:
	docker compose down

test:
	docker compose --profile test run --build --rm tests

client:
	docker compose --profile client run --build --rm tls-client

healthcare:
	docker compose --profile healthcare run --build --rm healthcare-client

verified:
	docker compose --profile verified run --build --rm verified-client

logs:
	docker compose logs -f tls-server

clean:
	docker compose down --rmi local -v
	rm -rf logs/
