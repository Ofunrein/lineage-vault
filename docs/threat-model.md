# Threat Model

## Assets

- Append-only lineage ledger (integrity-critical)
- Dataset and field lineage graph
- Idempotency index (duplicate prevention)

## Trust boundaries

| Boundary | Trust assumption |
|----------|------------------|
| API ingress | Untrusted clients; validate payloads, batch limits, request size |
| Storage file / DB | Host filesystem or managed Postgres trusted; protect credentials |
| Operators | Can read metrics/logs; no secret material in repo |

## Threats and mitigations

| Threat | Impact | Mitigation |
|--------|--------|------------|
| Ledger tampering | Audit loss | SHA-256 hash chain + `/verify` |
| Duplicate events | Inflated counts | Idempotency keys (header/body) |
| Partial writes / crash | Lost or orphan records | WAL staging + `recover_uncommitted()` per [crash-semantics.md](crash-semantics.md) |
| Schema-breaking transforms | Downstream corruption | Compliance agent + quarantine |
| PII in payloads | Privacy exposure | Forbidden-field quarantine (`ssn`, `raw_pan`, `password`) |
| Oversized batch/request | DoS / memory pressure | `LINEAGE_VAULT_MAX_BATCH_SIZE`, `LINEAGE_VAULT_MAX_REQUEST_BYTES`, 413 responses |
| Postgres credential exposure | DB compromise | DSN via environment only; never commit secrets |
| Secret leakage in repo | Credential exposure | CI + G7 secret/provenance scans |

## Out of scope (v0.2)

- Multi-tenant authN/authZ
- Cross-region replication
- Encrypted-at-rest database files
