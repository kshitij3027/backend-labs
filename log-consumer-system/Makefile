build:
	docker compose build

run:
	docker compose up -d redis app

stop:
	docker compose down

test:
	docker compose run --rm test

e2e:
	docker compose up -d redis app
	sleep 3
	docker compose run --rm e2e
	docker compose down

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f
