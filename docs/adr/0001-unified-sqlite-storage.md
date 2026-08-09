# ADR 0001: Unified SQLite Storage Backend

## Status

Accepted

## Context

The proof-of-concept used separate SQLite files for ledger, WAL, and graph stores. Portfolio depth requires transactional durability, concurrent writers, migrations, and idempotent ingestion.

## Decision

Use a single `SQLiteStorageBackend` implementing `StorageBackend` with:

- WAL journal mode and `synchronous=FULL`
- Schema migrations table
- Idempotency, ledger hash chain, WAL staging, dataset edges, field mappings, run events
- `threading.RLock` for writer serialization

## Crash semantics

See [crash-semantics.md](../crash-semantics.md). Summary:

- **Acknowledged writes** (`acknowledge_write`) survive process crash and DB close/reopen.
- **Staged writes** (`stage_partial`) may be promoted to ledger on `recover_uncommitted()` using the staged payload only.
- **Unstaged data** is never invented by recovery.

## Consequences

- Simpler crash recovery across related tables
- Legacy standalone ledger/WAL modules retained for focused unit tests
- Engine facades expose backward-compatible `.graph`, `.ledger`, `.wal` properties
