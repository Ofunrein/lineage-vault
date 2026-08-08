#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true

RESULTS=docs/gauntlet-results.md
echo "# Gauntlet Results — $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULTS"
echo "" >> "$RESULTS"

run_loop() {
  local name="$1"; shift
  echo "=== $name ==="
  if pytest "$@" -v --tb=short 2>&1 | tee /tmp/gauntlet_out.txt; then
    echo "## $name: PASS" >> "$RESULTS"
    grep -E "passed|PASSED" /tmp/gauntlet_out.txt | tail -1 >> "$RESULTS" || true
  else
    echo "## $name: FAIL" >> "$RESULTS"
    cat /tmp/gauntlet_out.txt >> "$RESULTS"
    exit 1
  fi
  echo "" >> "$RESULTS"
}

run_loop "L1 Ledger+WAL" tests/chaos/test_crash_recovery.py
run_loop "L2 TimeTravel" tests/unit/test_timetravel.py
run_loop "L3 Ingestion" tests/unit/test_ingestion.py
run_loop "L4 Schema+Compliance" tests/fixtures/test_schema_fixtures.py tests/unit/test_compliance.py
run_loop "L5 API" tests/integration/test_api.py

echo "ALL GAUNTLET LOOPS PASSED"
cat "$RESULTS"
