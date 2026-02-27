#!/bin/bash
set -e

echo "=== In-Container Verification ==="

# Test 1: CLI enrich command
echo "--- Test 1: CLI enrich ---"
RESULT=$(python -m src enrich "INFO: Application started" --source verify 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.load(sys.stdin)
assert 'message' in data, 'Missing message field'
assert 'hostname' in data, 'Missing hostname field'
assert 'service_name' in data, 'Missing service_name field'
assert 'timestamp' in data, 'Missing timestamp field'
print('PASS: CLI enrich works with expected fields')
"

# Test 2: ERROR log includes performance metrics
echo "--- Test 2: ERROR log performance metrics ---"
RESULT=$(python -m src enrich "ERROR: Critical failure occurred" --source verify 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.load(sys.stdin)
assert 'cpu_percent' in data, 'Missing cpu_percent'
assert 'memory_percent' in data, 'Missing memory_percent'
assert 'disk_percent' in data, 'Missing disk_percent'
print('PASS: ERROR logs include performance metrics')
"

# Test 3: Empty-ish message still produces valid output
echo "--- Test 3: Minimal message ---"
RESULT=$(python -m src enrich " " --source verify 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.load(sys.stdin)
assert 'message' in data
assert 'source' in data
assert 'timestamp' in data
print('PASS: Minimal message produces valid enriched log')
"

# Test 4: Batch processing
echo "--- Test 4: Batch processing ---"
TMPFILE=$(mktemp)
echo "INFO: First log" > "$TMPFILE"
echo "ERROR: Second log" >> "$TMPFILE"
echo "WARNING: Third log" >> "$TMPFILE"
RESULT=$(python -m src batch "$TMPFILE" --source verify 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.loads(sys.stdin.read())
assert isinstance(data, list), 'Expected list output'
assert len(data) == 3, f'Expected 3 items, got {len(data)}'
print('PASS: Batch processing works for 3 logs')
"
rm -f "$TMPFILE"

# Test 5: JSON serialization
echo "--- Test 5: JSON serialization ---"
RESULT=$(python -m src enrich "Test JSON serialization" 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.load(sys.stdin)
# Re-serialize to verify
json.dumps(data)
print('PASS: Output is valid, re-serializable JSON')
"

# Test 6: 8+ fields check
echo "--- Test 6: Field count check ---"
RESULT=$(python -m src enrich "INFO: Field count test" 2>/dev/null)
echo "$RESULT" | python -c "
import sys, json
data = json.load(sys.stdin)
count = len(data)
assert count >= 8, f'Expected 8+ fields, got {count}'
print(f'PASS: {count} fields in enriched output')
"

echo ""
echo "=== All in-container verification tests passed ==="
