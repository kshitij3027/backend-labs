#!/usr/bin/env bash
set -euo pipefail

echo "=== Log Format Compatibility Layer - Verification ==="
echo ""

# Test 1: Process mixed sample and check output
echo "Test 1: Processing mixed sample..."
python -m src process logs/samples/mixed_sample.txt --format json -o /tmp/verify_output
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: process command exited with code $EXIT_CODE"
    exit 1
fi
echo "PASS: process command succeeded"

# Test 2: Check output file exists and is valid JSON
echo ""
echo "Test 2: Validating output file..."
OUTPUT_FILE="/tmp/verify_output/mixed_sample_output.json"
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "FAIL: Output file not found"
    exit 1
fi

LINE_COUNT=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
if [ "$LINE_COUNT" -lt 8 ]; then
    echo "FAIL: Expected at least 8 output lines, got $LINE_COUNT"
    exit 1
fi

# Validate each line is valid JSON
while IFS= read -r line; do
    if ! python -c "import json; json.loads('$line')" 2>/dev/null; then
        # Try with proper escaping
        echo "$line" | python -c "import sys, json; json.loads(sys.stdin.readline())" 2>/dev/null || {
            echo "FAIL: Invalid JSON in output"
            exit 1
        }
    fi
done < "$OUTPUT_FILE"
echo "PASS: Output contains $LINE_COUNT valid JSON lines"

# Test 3: Check metrics
echo ""
echo "Test 3: Checking metrics..."
METRICS_FILE="/tmp/verify_output/mixed_sample_metrics.json"
if [ ! -f "$METRICS_FILE" ]; then
    echo "FAIL: Metrics file not found"
    exit 1
fi

# Extract metrics
SUCCESS_RATE=$(python -c "import json; m=json.load(open('$METRICS_FILE')); print(m['success_rate_percent'])")
THROUGHPUT=$(python -c "import json; m=json.load(open('$METRICS_FILE')); print(m['throughput_per_second'])")

echo "Success rate: ${SUCCESS_RATE}%"
echo "Throughput: ${THROUGHPUT} lines/sec"

# Check success rate >= 93%
python -c "
import json
m = json.load(open('$METRICS_FILE'))
assert m['success_rate_percent'] >= 93.0, f\"Success rate {m['success_rate_percent']}% < 93%\"
print('PASS: Success rate >= 93%')
"

# Check throughput >= 100 lines/sec
python -c "
import json
m = json.load(open('$METRICS_FILE'))
assert m['throughput_per_second'] >= 100, f\"Throughput {m['throughput_per_second']} < 100 lines/sec\"
print('PASS: Throughput >= 100 lines/sec')
"

# Test 4: Detection confidence
echo ""
echo "Test 4: Checking detection confidence..."
python -c "
from src.detection import FormatDetectionEngine

engine = FormatDetectionEngine()

# Test JSON detection
result = engine.detect_line('{\"level\": \"INFO\", \"message\": \"test\"}')
assert result is not None, 'JSON not detected'
fmt, conf = result
assert conf >= 0.9, f'JSON confidence {conf} < 0.9'
print(f'  JSON confidence: {conf:.2f} - PASS')

# Test syslog RFC 3164
result = engine.detect_line('<34>Oct 11 22:14:15 mymachine su: test')
assert result is not None, 'RFC 3164 not detected'
fmt, conf = result
assert conf >= 0.85, f'RFC 3164 confidence {conf} < 0.85'
print(f'  RFC 3164 confidence: {conf:.2f} - PASS')

# Test syslog RFC 5424
result = engine.detect_line('<165>1 2003-10-11T22:14:15.003Z host app - ID47 - test')
assert result is not None, 'RFC 5424 not detected'
fmt, conf = result
assert conf >= 0.9, f'RFC 5424 confidence {conf} < 0.9'
print(f'  RFC 5424 confidence: {conf:.2f} - PASS')

# Test journald
result = engine.detect_line('Feb 14 06:36:01 myhost systemd[1]: Started test.service')
assert result is not None, 'Journald not detected'
fmt, conf = result
assert conf >= 0.5, f'Journald confidence {conf} < 0.5'
print(f'  Journald confidence: {conf:.2f} - PASS')

print('PASS: All confidence thresholds met')
"

# Test 5: Format detection
echo ""
echo "Test 5: Batch detection..."
python -c "
from src.detection import FormatDetectionEngine

engine = FormatDetectionEngine()
lines = open('logs/samples/mixed_sample.txt').read().strip().split('\n')
result = engine.detect_batch(lines)
assert result['detection_rate'] >= 0.9, f\"Detection rate {result['detection_rate']} < 90%\"
print(f\"  Detection rate: {result['detection_rate']:.0%}\")
print(f\"  Formats found: {list(result['formats'].keys())}\")
print('PASS: Batch detection rate >= 90%')
"

echo ""
echo "=== ALL VERIFICATION CHECKS PASSED ==="
