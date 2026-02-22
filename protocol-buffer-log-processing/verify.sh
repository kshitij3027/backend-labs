#!/bin/bash
# verify.sh — Comprehensive verification script for Protocol Buffer Log Processing
set -e

# ---------------------------------------------------------------------------
# Color helpers (with fallback for non-color terminals)
# ---------------------------------------------------------------------------
if [ -t 1 ] && command -v tput >/dev/null 2>&1 && [ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]; then
    GREEN=$(tput setaf 2)
    RED=$(tput setaf 1)
    CYAN=$(tput setaf 6)
    BOLD=$(tput bold)
    RESET=$(tput sgr0)
else
    GREEN=""
    RED=""
    CYAN=""
    BOLD=""
    RESET=""
fi

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "  ${GREEN}[PASS]${RESET} $1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "  ${RED}[FAIL]${RESET} $1"
}

banner() {
    echo ""
    echo "${BOLD}${CYAN}======================================================================${RESET}"
    echo "${BOLD}${CYAN}  PROTOCOL BUFFER LOG PROCESSING — VERIFICATION${RESET}"
    echo "${BOLD}${CYAN}======================================================================${RESET}"
    echo ""
}

section() {
    echo ""
    echo "${BOLD}--- $1 ---${RESET}"
}

# ---------------------------------------------------------------------------
banner
# ---------------------------------------------------------------------------

# ==========================================================================
# Check 1: Project structure
# ==========================================================================
section "Check 1: Project Structure"

REQUIRED_FILES=(
    "proto/log_entry.proto"
    "src/__init__.py"
    "src/serializer.py"
    "src/validator.py"
    "src/log_generator.py"
    "src/benchmark.py"
    "src/report.py"
    "src/config.py"
    "main.py"
    "Dockerfile"
    "Dockerfile.test"
    "docker-compose.yml"
    "requirements.txt"
    "compile_proto.sh"
    "tests/test_serializer.py"
    "tests/test_validator.py"
    "tests/test_log_generator.py"
    "tests/test_benchmark.py"
)

for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$f" ]; then
        pass "$f exists"
    else
        fail "$f is missing"
    fi
done

# ==========================================================================
# Check 2: Python environment
# ==========================================================================
section "Check 2: Python Environment"

if python -c "import google.protobuf" 2>/dev/null; then
    pass "protobuf is importable"
else
    fail "protobuf is NOT importable"
fi

if python -c "import pytest" 2>/dev/null; then
    pass "pytest is importable"
else
    fail "pytest is NOT importable"
fi

# ==========================================================================
# Check 3: Proto compilation
# ==========================================================================
section "Check 3: Proto Compilation"

if python -c "from src.generated.log_entry_pb2 import LogEntry; print(f'  LogEntry fields: {[f.name for f in LogEntry.DESCRIPTOR.fields]}')" 2>/dev/null; then
    pass "Proto compiled — LogEntry importable"
else
    fail "Proto compilation broken — cannot import LogEntry"
fi

if python -c "from src.generated.log_entry_pb2 import LogBatch" 2>/dev/null; then
    pass "Proto compiled — LogBatch importable"
else
    fail "Proto compilation broken — cannot import LogBatch"
fi

# ==========================================================================
# Check 4: Unit tests
# ==========================================================================
section "Check 4: Unit Tests"

if python -m pytest tests/ -v --tb=short 2>&1; then
    pass "All unit tests passed"
else
    fail "Some unit tests failed (see output above)"
fi

# ==========================================================================
# Check 5: Main pipeline
# ==========================================================================
section "Check 5: Main Pipeline"

# Clean up any previous log files so we verify fresh output
rm -rf logs/json logs/protobuf

if python main.py 2>&1; then
    pass "main.py completed successfully"
else
    fail "main.py exited with an error"
fi

# ==========================================================================
# Check 6: Log files created
# ==========================================================================
section "Check 6: Log File Output"

JSON_FILE="logs/json/batch.json"
PROTO_FILE="logs/protobuf/batch.pb"

if [ -f "$JSON_FILE" ] && [ -s "$JSON_FILE" ]; then
    pass "$JSON_FILE exists and is non-empty"
else
    fail "$JSON_FILE is missing or empty"
fi

if [ -f "$PROTO_FILE" ] && [ -s "$PROTO_FILE" ]; then
    pass "$PROTO_FILE exists and is non-empty"
else
    fail "$PROTO_FILE is missing or empty"
fi

# ==========================================================================
# Check 7: Protobuf is smaller than JSON
# ==========================================================================
section "Check 7: Size Comparison"

if [ -f "$JSON_FILE" ] && [ -f "$PROTO_FILE" ]; then
    JSON_SIZE=$(wc -c < "$JSON_FILE" | tr -d ' ')
    PROTO_SIZE=$(wc -c < "$PROTO_FILE" | tr -d ' ')

    echo "  JSON file size  : ${JSON_SIZE} bytes"
    echo "  Proto file size : ${PROTO_SIZE} bytes"

    if [ "$PROTO_SIZE" -lt "$JSON_SIZE" ]; then
        SAVINGS=$(( (JSON_SIZE - PROTO_SIZE) * 100 / JSON_SIZE ))
        pass "Protobuf is smaller than JSON (${SAVINGS}% savings)"
    else
        fail "Protobuf is NOT smaller than JSON"
    fi
else
    fail "Cannot compare sizes — log files missing"
fi

# ==========================================================================
# Summary
# ==========================================================================
echo ""
echo "${BOLD}${CYAN}======================================================================${RESET}"
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo "${BOLD}  RESULTS: ${GREEN}${PASS_COUNT} passed${RESET}${BOLD}, ${RED}${FAIL_COUNT} failed${RESET}${BOLD} (${TOTAL} total)${RESET}"

if [ "$FAIL_COUNT" -eq 0 ]; then
    echo "${BOLD}${GREEN}  STATUS: ALL CHECKS PASSED${RESET}"
    echo "${BOLD}${CYAN}======================================================================${RESET}"
    echo ""
    exit 0
else
    echo "${BOLD}${RED}  STATUS: SOME CHECKS FAILED${RESET}"
    echo "${BOLD}${CYAN}======================================================================${RESET}"
    echo ""
    exit 1
fi
