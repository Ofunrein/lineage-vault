# LineageVault

Open-source data lineage and tamper-evident audit ledger with OpenLineage-compatible ingestion.

## Features

- Pluggable storage: SQLite (default) or PostgreSQL
- Transactional batch ingestion with idempotency and hash-chained ledger
- OpenLineage `RunEvent` ingestion with field-level lineage
- Hardened API: `/ready`, batch ingest, bounded payloads, stable errors
- Comparative benchmark harness (sqlite single/batch, postgres when available)
- Property/adversarial, failure-injection, and load tests

## Quick start (5 commands)

```bash
pip install -e ".[dev,postgres]"
lineage-vault demo --data-dir .data-demo
lineage-vault verify --data-dir .data-demo
lineage-vault serve
curl -s localhost:8000/ready | jq .
curl -s localhost:8000/impact/field/warehouse.raw_orders/amount | jq .
```

## Backend selection

| Variable | Default | Description |
|----------|---------|-------------|
| `LINEAGE_VAULT_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `LINEAGE_VAULT_POSTGRES_DSN` | — | Required when backend is `postgres` |
| `LINEAGE_VAULT_PG_POOL_MIN` | `1` | Postgres pool minimum connections |
| `LINEAGE_VAULT_PG_POOL_MAX` | `10` | Postgres pool maximum connections |
| `LINEAGE_VAULT_MAX_REQUEST_BYTES` | `1048576` | Max HTTP request body size |

## Demo workflow

```bash
# Run built-in ETL demo (OpenLineage + legacy event)
lineage-vault demo --data-dir .data-demo

# Single-mode benchmark (default sqlite-single)
lineage-vault benchmark --events 10000 --output docs/benchmark-results.json

# Comparative benchmark (postgres modes skipped if server unavailable)
lineage-vault benchmark --mode compare --events 5000 --output docs/benchmark-comparative.json

# Portfolio gauntlet (G1-G7)
bash scripts/run_portfolio_gauntlet.sh
```

## API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health + integrity |
| `GET /ready` | Readiness (503 if storage/integrity unavailable) |
| `GET /metrics` | Prometheus metrics |
| `POST /openlineage` | OpenLineage RunEvent ingest |
| `POST /openlineage/batch` | Bounded batch ingest |
| `POST /events` | Legacy lineage event ingest |
| `GET /lineage/{dataset}` | Upstream/downstream datasets |
| `GET /impact/field/{dataset}/{field}` | Field-level downstream impact |
| `POST /demo/run` | Execute demo pipeline |

## Docs

- [Architecture](docs/architecture.md)
- [Threat model](docs/threat-model.md)
- [ADR 0001: SQLite storage](docs/adr/0001-unified-sqlite-storage.md)
- [ADR 0002: Pluggable backends](docs/adr/0002-pluggable-storage-backends.md)
- [Crash semantics](docs/crash-semantics.md)

## Docker

```bash
# PostgreSQL-backed stack (default)
docker compose up --build
curl localhost:8000/ready

# SQLite profile
docker compose --profile sqlite up lineage-vault-sqlite --build
```
