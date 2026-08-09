#!/usr/bin/env bash
# Portfolio-depth Gauntlet gates G1-G7
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true

RESULTS=docs/portfolio-gauntlet-results.md
SCAN_PAT='apple|sds|floodgate|strategic data|cursoragent|pie\.apple|genai\.apple|internal provenance|apple sds|co-authored-by: cursor'
SECRET_PAT='sk-[A-Za-z0-9]{10,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]+|AKIA[0-9A-Z]{16}|BEGIN (RSA |OPENSSH )PRIVATE'

pass() { echo "## $1: PASS" >> "$RESULTS"; echo "=== $1 PASS ==="; }
fail() { echo "## $1: FAIL" >> "$RESULTS"; echo "=== $1 FAIL ==="; cat "${2:-/dev/stdin}"; exit 1; }

echo "# Portfolio Gauntlet — $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULTS"

# G1 — full pytest + property tests
echo "Running G1..."
if pytest -v --tb=short 2>&1 | tee /tmp/g1.out; then
  pass "G1 pytest+property"
else
  fail "G1 pytest+property" /tmp/g1.out
fi

# G2 — crash/corruption recovery
echo "Running G2..."
if pytest tests/unit/test_storage.py tests/chaos/test_crash_recovery.py tests/chaos/test_storage_reopen_recovery.py tests/property/test_adversarial.py -v --tb=short 2>&1 | tee /tmp/g2.out; then
  pass "G2 crash recovery"
else
  fail "G2 crash recovery" /tmp/g2.out
fi

# G3 — OpenLineage e2e + field impact
echo "Running G3..."
if pytest tests/integration/test_openlineage_e2e.py tests/unit/test_storage.py::test_field_impact_traversal -v --tb=short 2>&1 | tee /tmp/g3.out; then
  pass "G3 OpenLineage e2e"
else
  fail "G3 OpenLineage e2e" /tmp/g3.out
fi

# G4 — comparative benchmark (sqlite single+batch; postgres when available)
echo "Running G4..."
EVENTS=${BENCH_EVENTS:-10000}
BATCH_SIZE=${BENCH_BATCH_SIZE:-50}
python -c "
from lineage_vault.benchmark.harness import run_comparative_benchmark
import json
payload = run_comparative_benchmark(
    data_dir='.data-bench',
    events=$EVENTS,
    workers=8,
    batch_size=$BATCH_SIZE,
    output='docs/benchmark-comparative.json',
)
# Keep legacy single-mode artifact for dashboards
with open('docs/benchmark-results.json', 'w') as f:
    json.dump(payload['results']['sqlite-single'], f, indent=2)
    f.write('\n')
sqlite_single = payload['results']['sqlite-single']
sqlite_batch = payload['results']['sqlite-batch']
assert not sqlite_single.get('skipped'), 'sqlite-single must run'
assert sqlite_single['integrity_ok'], 'sqlite-single integrity failed'
assert sqlite_single['acknowledged_writes'] == $EVENTS, sqlite_single
assert sqlite_single['throughput_eps'] > 0
assert not sqlite_batch.get('skipped'), 'sqlite-batch must run'
assert sqlite_batch['integrity_ok'], 'sqlite-batch integrity failed'
assert sqlite_batch['acknowledged_writes'] == $EVENTS, sqlite_batch
assert sqlite_batch['throughput_eps'] > 0
for mode in ('postgres-single', 'postgres-batch'):
    pg = payload['results'][mode]
    if pg.get('skipped'):
        assert pg.get('skip_reason'), f'{mode} skipped without reason'
    else:
        assert pg['integrity_ok'], f'{mode} integrity failed'
        assert pg['acknowledged_writes'] == $EVENTS, pg
print(json.dumps({
    'sqlite_single_eps': sqlite_single['throughput_eps'],
    'sqlite_batch_eps': sqlite_batch['throughput_eps'],
    'postgres_single_skipped': payload['results']['postgres-single'].get('skipped', False),
    'postgres_batch_skipped': payload['results']['postgres-batch'].get('skipped', False),
}, indent=2))
" 2>&1 | tee /tmp/g4.out
pass "G4 comparative benchmark ${EVENTS} events"

# G5 — API/CLI demo <=5 commands
echo "Running G5..."
rm -rf .data-g5
python -m lineage_vault.cli demo --data-dir .data-g5 2>&1 | tee /tmp/g5a.out
python -m lineage_vault.cli verify --data-dir .data-g5 2>&1 | tee /tmp/g5b.out
python -c "
from fastapi.testclient import TestClient
from lineage_vault.api.app import create_app
c = TestClient(create_app('.data-g5'))
assert c.get('/health').status_code == 200
assert c.get('/impact/field/warehouse.raw_orders/amount').status_code == 200
assert c.post('/verify').json()['valid'] is True
print('workflow ok')
" 2>&1 | tee /tmp/g5c.out
pass "G5 demo workflow"

# G6 — Docker build/start/health or config validation
echo "Running G6..."
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker build -t lineage-vault:gauntlet . 2>&1 | tee /tmp/g6.out
  cid=$(docker run -d -p 18000:8000 lineage-vault:gauntlet)
  trap "docker rm -f $cid >/dev/null 2>&1 || true" EXIT
  for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:18000/health >/dev/null; then break; fi
    sleep 1
  done
  curl -sf http://127.0.0.1:18000/health | tee /tmp/g6health.out
  docker rm -f "$cid" >/dev/null
  trap - EXIT
  pass "G6 docker health"
else
  test -f Dockerfile && test -f docker-compose.yml
  grep -q 'lineage-vault' Dockerfile
  grep -q 'serve' Dockerfile
  grep -q '8000:8000' docker-compose.yml
  pass "G6 docker config validation (docker unavailable)"
fi

# G7 — security + provenance scan (exclude this runner's pattern definitions)
echo "Running G7..."
if rg -i -n "$SCAN_PAT" . \
  --glob '!.git' --glob '!.venv/**' --glob '!.data*' \
  --glob '!scripts/run_portfolio_gauntlet.sh' 2>/dev/null; then
  fail "G7 provenance tree scan"
fi
if git log --all --format='%H %s%n%b' | rg -i "$SCAN_PAT"; then
  fail "G7 provenance history scan"
fi
if git grep -i -e apple -e sds -e floodgate $(git rev-list --all) -- ':!scripts/run_portfolio_gauntlet.sh' 2>/dev/null; then
  fail "G7 provenance blob scan"
fi
if git log --all -p | rg "$SECRET_PAT"; then
  fail "G7 secret history scan"
fi
if rg "$SECRET_PAT" . --glob '!.git' --glob '!.venv/**' 2>/dev/null; then
  fail "G7 secret tree scan"
fi
pass "G7 security+provenance"

echo "ALL PORTFOLIO GAUNTLET GATES PASSED"
cat "$RESULTS"
