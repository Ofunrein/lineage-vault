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
    SB[(SQLite WAL Backend)]
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
  ENG --> SB
  IMP --> SB
  SB --> LED
  SB --> WAL
  SB --> IDEM
  SB --> GRP
  SB --> FL
  API --> MET
  ENG --> LOG
```

## Components

- **Storage interface** — transactional acknowledge semantics with idempotency keys.
- **SQLite backend** — single-database WAL mode, migrations, indexes, crash replay.
- **OpenLineage adapter** — ingests `RunEvent` payloads, records dataset + field lineage.
- **Impact analyzer** — downstream dataset and field traversal.
- **API/CLI** — health, metrics, ingest, lineage, impact, demo workflow.

## Data flow

1. Client sends OpenLineage `COMPLETE` event (or legacy event).
2. Storage acknowledges write in one transaction (idempotency + WAL + ledger).
3. Dataset edges and field mappings recorded for graph queries.
4. Impact API traverses downstream dependencies.
