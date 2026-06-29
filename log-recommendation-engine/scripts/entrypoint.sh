#!/bin/sh
# Container entrypoint for the API service.
#
# Applies any pending Alembic migrations against Postgres (only when alembic.ini is
# present), then hands off to the process passed as arguments (uvicorn by default).
# The migration step is GUARDED so this same entrypoint works in C1 (no alembic.ini
# yet) and in C2+ (migrations land). Postgres is guaranteed ready by the compose
# `depends_on: postgres: condition: service_healthy`, so `alembic upgrade head` can
# run straight away.
#
# `set -e` makes a failed migration abort startup loudly instead of booting an app
# against an out-of-date schema.
set -e

if [ -f alembic.ini ]; then
  echo "[entrypoint] alembic upgrade head"
  alembic upgrade head
fi

exec "$@"
