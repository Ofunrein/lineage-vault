# ADR 0002: Pluggable Storage Backends and Batch Ingestion

## Status

Accepted

## Context

Portfolio v0.2 standardized on a single SQLite backend. Production deployments need:

- Higher throughput via durable batch writes without weakening idempotency or hash-chain integrity
- Optional PostgreSQL for multi-writer deployments and managed database hosting
- Explicit backend selection and comparative benchmarks (no fabricated cross-backend numbers)

## Decision

1. Extend `StorageBackend` with `acknowledge_writes_batch()`; SQLite and PostgreSQL provide transactional batch implementations.
2. Add `StorageConfig` + `create_storage_backend()` driven by `LINEAGE_VAULT_BACKEND` and `LINEAGE_VAULT_POSTGRES_DSN`.
3. Implement `PostgresStorageBackend` with the same schema semantics as SQLite (migrations, WAL staging, idempotency, ledger chain) and a **bounded `psycopg_pool.ConnectionPool`** (default max 10). Ledger writes serialize via `pg_advisory_xact_lock`; reads use pooled connections concurrently.
4. Harden API ingress: `/ready`, `/openlineage/batch`, bounded batch size, request-size limits, stable error envelopes.
5. Expand benchmark harness with `sqlite-single`, `sqlite-batch`, `postgres-*` modes; mark postgres runs `skipped` when no server is reachable.

## Consequences

- Engine and OpenLineage ingest use storage interface only (no SQLite coupling).
- Docker Compose and CI run PostgreSQL integration tests when DSN is available.
- Comparative benchmarks may include `skipped: true` entries — this is expected and honest.
