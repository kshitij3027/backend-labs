#!/usr/bin/env bash
set -e

BASE_URL="${1:-http://localhost:5050}"

echo "Health checking $BASE_URL ..."

echo -n "  /health ... "
curl -sf "$BASE_URL/health" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['status']=='healthy'; print('OK')"

echo -n "  /api/schema-info ... "
curl -sf "$BASE_URL/api/schema-info" | python3 -c "
import sys,json
d=json.load(sys.stdin)
schemas=d['data']['available_schemas']
matrix=d['data']['compatibility_matrix']
total=sum(1 for w in matrix for r in matrix[w] if matrix[w][r])
print(f'OK - {len(schemas)} schemas, {total}/9 compatible')
"

echo "All health checks passed."
