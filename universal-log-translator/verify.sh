#!/bin/bash
set -e

# Ensure src package is importable from /app
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}/app"
cd /app

echo "=== Universal Log Translator E2E Verification ==="
echo ""

# Generate binary sample files if needed
echo "--- Generating sample files ---"
python scripts/generate_samples.py

# Test translate command with each format
echo ""
echo "--- Testing translate command ---"

for file in sample_logs/sample.json sample_logs/sample.txt sample_logs/sample.pb sample_logs/sample.avro; do
    echo ""
    echo "Translating: $file"
    python -m src translate "$file" || { echo "FAIL: $file"; exit 1; }
    echo "OK"
done

# Test detect command
echo ""
echo "--- Testing detect command ---"

for file in sample_logs/sample.json sample_logs/sample.txt sample_logs/sample.pb sample_logs/sample.avro; do
    echo "Detecting: $file"
    python -m src detect "$file" || { echo "FAIL: detect $file"; exit 1; }
done

# Test text output format
echo ""
echo "--- Testing text output format ---"
python -m src translate --output text sample_logs/sample.json || { echo "FAIL: text output"; exit 1; }
echo "OK: text output"

# Test auto-detection via stdin
echo ""
echo "--- Testing stdin input ---"
cat sample_logs/sample.json | python -m src translate - || { echo "FAIL: stdin"; exit 1; }
echo "OK: stdin"

# Test format hint
echo ""
echo "--- Testing format hint ---"
python -m src translate --format json sample_logs/sample.json || { echo "FAIL: format hint"; exit 1; }
echo "OK: format hint"

echo ""
echo "=== All E2E tests PASSED ==="
