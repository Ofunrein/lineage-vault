# Crash and Recovery Semantics

This document defines durable-write contracts for `SQLiteStorageBackend`. Tests in
`tests/chaos/test_storage_reopen_recovery.py` are the authoritative verifier.

## Write classes

| Class | API | Durability | After crash + reopen |
|-------|-----|------------|----------------------|
| **Acknowledged** | `acknowledge_write()` | Idempotency row + WAL row (`committed=1`) + ledger row in one transaction | **Must survive** without calling `recover_uncommitted()` |
| **Staged** | `stage_partial()` | WAL row (`committed=0`) with exact `event_id` + payload JSON | **May** be completed by `recover_uncommitted()` using **only** the staged payload |
| **Unstaged** | (none) | Not durable | **Must never** appear in ledger or idempotency tables |

## Invariants

1. **No invented data** — recovery never creates ledger rows for event IDs that lack a durable WAL staging row.
2. **Payload fidelity** — recovered ledger entries use the JSON stored in `wal_staging` at recovery time; payloads are not synthesized or merged.
3. **Idempotency survival** — acknowledged idempotency keys remain mapped to the original `event_id` across close/reopen; duplicate acknowledges return the original event without a second ledger row.
4. **Hash-chain integrity** — `verify_integrity()` is true after reopen and after recovery; tampering is detected.
5. **Recovery idempotence** — a second `recover_uncommitted()` returns `0` and does not change ledger cardinality.

## Non-goals

- Staged-but-unacknowledged events do **not** receive idempotency rows during recovery (only ledger completion).
- Graph edges and field mappings are not replayed by storage recovery (engine-level concern).
