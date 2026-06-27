#!/bin/sh
# Container entrypoint for the API service.
#
# Applies any pending Alembic migrations against Postgres, then hands off to the
# process passed as arguments (uvicorn by default). Postgres is guaranteed ready
# by the compose `depends_on: postgres: condition: service_healthy`, so
# `alembic upgrade head` can run straight away.
#
# `set -e` makes a failed migration abort startup loudly instead of booting an
# app against an out-of-date schema.
set -e

echo "[entrypoint] applying database migrations (alembic upgrade head)..."
alembic upgrade head
echo "[entrypoint] migrations applied; starting application..."

exec "$@"
