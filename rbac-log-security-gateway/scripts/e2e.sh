#!/usr/bin/env bash
# End-to-end smoke runner for the RBAC Log Security Gateway.
# Builds + starts the stack, runs the pytest suite inside the tester container,
# walks the 4 demo users through a curl matrix against the live API (via nginx),
# verifies key role/tag behaviors, then tears down.
#
# Designed to fail loudly on the first error. Returns non-zero on any check that
# diverges from the locked role table.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

BACKEND="${BACKEND:-http://localhost:8000}"
FRONTEND="${FRONTEND:-http://localhost:3000}"
TEST_SECRET="${JWT_SECRET_KEY:-e2e-test-secret-that-is-long-enough}"

log()  { echo "[e2e] $*"; }
fail() { echo "[e2e][FAIL] $*" >&2; exit 1; }

cleanup() {
  log "tearing down stack"
  docker compose down >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- Step 1: build + start ---
log "building images"
docker compose build backend frontend tester >/dev/null

log "starting stack with JWT_SECRET_KEY=*** (test override)"
JWT_SECRET_KEY="$TEST_SECRET" docker compose up -d --wait backend frontend

# --- Step 2: pytest in Docker ---
log "running full pytest suite in tester container"
docker compose run --rm tester pytest -q

# --- Step 3: helpers ---
login() {
  local username="$1" password="$2"
  curl -fsS -X POST "$BACKEND/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$username\",\"password\":\"$password\"}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])"
}

assert_status() {
  local label="$1" method="$2" url="$3" token="$4" expected="$5"
  local actual
  if [[ "$method" == "GET" ]]; then
    actual=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $token" "$url")
  else
    actual=$(curl -s -o /dev/null -w "%{http_code}" -X "$method" -H "Authorization: Bearer $token" "$url")
  fi
  if [[ "$actual" != "$expected" ]]; then
    fail "$label: expected $expected got $actual ($method $url)"
  fi
  log "  PASS $label: $actual"
}

# --- Step 4: walk the 4 demo users through a representative matrix ---
log "logging in as 4 demo users"
ADMIN=$(login alice admin123)
DEV=$(login bob   dev123)
ANALYST=$(login carol analyst123)
SUPPORT=$(login dave  support123)

log "admin matrix"
assert_status "admin reads application.auth"        GET "$BACKEND/api/logs/search?resource=application.auth"   "$ADMIN" 200
assert_status "admin reads business.financial"      GET "$BACKEND/api/logs/search?resource=business.financial" "$ADMIN" 200
assert_status "admin exports business.financial (the symbolic deny)" GET "$BACKEND/api/logs/export?resource=business.financial" "$ADMIN" 403
assert_status "admin audit-summary"                 GET "$BACKEND/api/admin/audit-summary"                     "$ADMIN" 200

log "developer matrix"
assert_status "dev reads application.auth"          GET "$BACKEND/api/logs/search?resource=application.auth"   "$DEV"   200
assert_status "dev denied business.metrics"         GET "$BACKEND/api/logs/search?resource=business.metrics"   "$DEV"   403
assert_status "dev exports application.api"         GET "$BACKEND/api/logs/export?resource=application.api"    "$DEV"   200
assert_status "dev denied admin endpoint"           GET "$BACKEND/api/admin/audit-summary"                     "$DEV"   403

log "analyst matrix"
assert_status "analyst reads business.metrics"      GET "$BACKEND/api/logs/search?resource=business.metrics"   "$ANALYST" 200
assert_status "analyst denied business.customer"    GET "$BACKEND/api/logs/search?resource=business.customer"  "$ANALYST" 403
assert_status "analyst exports business.metrics"    GET "$BACKEND/api/logs/export?resource=business.metrics"   "$ANALYST" 200
assert_status "analyst denied admin endpoint"       GET "$BACKEND/api/admin/audit-summary"                     "$ANALYST" 403

log "support matrix"
assert_status "support reads business.customer"     GET "$BACKEND/api/logs/search?resource=business.customer"  "$SUPPORT" 200
assert_status "support denied system.kernel"        GET "$BACKEND/api/logs/search?resource=system.kernel"      "$SUPPORT" 403
assert_status "support denied any export"           GET "$BACKEND/api/logs/export?resource=application.auth"   "$SUPPORT" 403
assert_status "support denied admin endpoint"       GET "$BACKEND/api/admin/audit-summary"                     "$SUPPORT" 403

# --- Step 5: tag semantics — analyst aggregated, support masked ---
log "verifying analyst aggregated_only response shape"
RESPONSE=$(curl -fsS -H "Authorization: Bearer $ANALYST" "$BACKEND/api/logs/search?resource=business.metrics")
python3 -c "
import sys, json
data = json.loads('''$RESPONSE''')
assert data.get('records') is None, f'analyst should not see records, got {data}'
assert data.get('aggregated') is not None, f'analyst should see aggregated, got {data}'
assert 'by_level' in data['aggregated']
print('[e2e]   PASS analyst aggregated shape')
"

log "verifying support mask_pii response shape"
RESPONSE=$(curl -fsS -H "Authorization: Bearer $SUPPORT" "$BACKEND/api/logs/search?resource=business.customer")
python3 -c "
import sys, json
data = json.loads('''$RESPONSE''')
assert data.get('masked') is True, f'support should be masked, got {data}'
PII = {'email','ip','phone','user_id','username'}
for rec in data.get('records') or []:
  for k in rec.get('fields') or {}:
    if k in PII and rec['fields'][k] != '***':
      raise AssertionError(f'unmasked {k}={rec[\"fields\"][k]} on record {rec[\"id\"]}')
print('[e2e]   PASS support mask_pii shape')
"

# --- Step 6: frontend serves HTML ---
log "checking frontend HTML"
HTML=$(curl -fsS "$FRONTEND/")
echo "$HTML" | grep -q '<div id="root">' || fail "frontend index HTML missing #root"
echo "$HTML" | grep -q "RBAC Log Security Gateway"  || fail "frontend index missing title"
log "  PASS frontend serves SPA shell"

# --- Step 7: nginx proxy /api/health and /api/auth/login both routing to backend ---
log "checking nginx /api proxy"
curl -fsS "$FRONTEND/health" >/dev/null
TOK=$(login alice admin123)
curl -fsS -o /dev/null -w "  PASS profile through proxy: %{http_code}\n" -H "Authorization: Bearer $TOK" "$FRONTEND/api/auth/profile"

log "ALL CHECKS PASSED"
