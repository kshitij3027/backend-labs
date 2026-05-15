#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

ENV_FILE="$PROJECT_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[start.sh] .env not found — generating one with a random JWT secret"
  SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
  cat > "$ENV_FILE" <<EOF
# Auto-generated on first run. Safe to edit. Never commit.
JWT_SECRET_KEY=$SECRET
JWT_ALGORITHM=HS256
JWT_EXPIRY_MINUTES=60
APP_HOST=0.0.0.0
APP_PORT=8000
APP_LOG_LEVEL=info
CORS_ALLOWED_ORIGINS=http://localhost:3000
EOF
  chmod 600 "$ENV_FILE"
  echo "[start.sh] .env created with random JWT secret"
else
  echo "[start.sh] using existing .env"
fi

echo "[start.sh] building and starting backend + frontend"
docker compose up -d --wait backend frontend
echo "[start.sh] backend ready at http://localhost:8000  (docs at /docs)"
echo "[start.sh] frontend ready at http://localhost:3000"
