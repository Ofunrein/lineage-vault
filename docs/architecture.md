# Architecture

```mermaid
flowchart TB
  subgraph clients [Clients]
    CLI[CLI]
    API[FastAPI]
    Demo[Demo Pipeline]
  end

  subgraph core [LineageVault Core]
    OL[OpenLineage Ingestor]
    ENG[Engine]
    IMP[Impact Analyzer]
    CMP[Compliance Agent]
  end

  subgraph storage [Durable Storage]
    IF[StorageBackend interface]
    SQL[(SQLite Backend)]
    PG[(PostgreSQL Backend)]
    LED[Hash Ledger]
    WAL[Staging WAL]
    IDEM[Idempotency Index]
    GRP[Dataset Graph]
    FL[Field Mappings]
  end

  subgraph obs [Observability]
    LOG[Structured JSON Logs]
    MET[Prometheus Metrics]
  end

  CLI --> ENG
  API --> ENG
  Demo --> OL
  OL --> ENG
  ENG --> IF
  IF --> SQL
  IF --> PG
  IMP --> IF
  SQL --> LED
  SQL --> WAL
  PG --> LED
  PG --> WAL
  API --> MET
  ENG --> LOG
```

## Components

- **Storage interface** — transactional acknowledge + batch semantics with idempotency keys.
- **SQLite backend** — WAL journal mode, migrations, batch writes, crash replay.
- **PostgreSQL backend** — same contract with connection pooling via psycopg, transactional batch ingest.
- **OpenLineage adapter** — single and batch `RunEvent` ingestion with dataset + field lineage.
- **Impact analyzer** — downstream dataset and field traversal.
- **API/CLI** — health, readiness, metrics, ingest, lineage, impact, demo workflow.

## Data flow

1. Client sends OpenLineage `COMPLETE` event (single or batch).
2. Storage acknowledges writes in one transaction (idempotency + WAL + ledger).
3. Dataset edges and field mappings recorded for graph queries.
4. Impact API traverses downstream dependencies.

## Configuration

Backend selection via `LINEAGE_VAULT_BACKEND` and `LINEAGE_VAULT_POSTGRES_DSN`. See [ADR 0002](adr/0002-pluggable-storage-backends.md).
