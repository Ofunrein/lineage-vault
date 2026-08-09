# LineageVault

Open-source data lineage and tamper-evident audit ledger with OpenLineage-compatible ingestion.

## Features

- Transactional SQLite storage (migrations, idempotency, crash recovery)
- Hash-chained append-only ledger
- OpenLineage `RunEvent` ingestion with field-level lineage
- Downstream dataset and field impact analysis
- Structured JSON logs + Prometheus metrics
- Property/adversarial tests and benchmark harness

## Quick start (5 commands)

```bash
pip install -e ".[dev]"
lineage-vault demo --data-dir .data-demo
lineage-vault verify --data-dir .data-demo
lineage-vault serve
curl -s localhost:8000/health | jq .
curl -s localhost:8000/impact/field/warehouse.raw_orders/amount | jq .
```

## Demo workflow

```bash
# Run built-in ETL demo (OpenLineage + legacy event)
lineage-vault demo --data-dir .data-demo

# Benchmark 10k concurrent writes
lineage-vault benchmark --events 10000 --output docs/benchmark-results.json

# Portfolio gauntlet (G1-G7)
bash scripts/run_portfolio_gauntlet.sh
```

## API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health + integrity |
| `GET /metrics` | Prometheus metrics |
| `POST /openlineage` | OpenLineage RunEvent ingest |
| `POST /events` | Legacy lineage event ingest |
| `GET /lineage/{dataset}` | Upstream/downstream datasets |
| `GET /impact/field/{dataset}/{field}` | Field-level downstream impact |
| `POST /demo/run` | Execute demo pipeline |

## Docs

- [Architecture](docs/architecture.md)
- [Threat model](docs/threat-model.md)
- [ADR 0001: Storage backend](docs/adr/0001-unified-sqlite-storage.md)

## Docker

```bash
docker compose up --build
curl localhost:8000/health
```
