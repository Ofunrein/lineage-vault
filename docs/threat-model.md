# Threat Model

## Assets

- Append-only lineage ledger (integrity-critical)
- Dataset and field lineage graph
- Idempotency index (duplicate prevention)

## Trust boundaries

| Boundary | Trust assumption |
|----------|------------------|
| API ingress | Untrusted clients; validate payloads |
| Storage file | Host filesystem trusted; protect file permissions |
| Operators | Can read metrics/logs; no secret material in repo |

## Threats and mitigations

| Threat | Impact | Mitigation |
|--------|--------|------------|
| Ledger tampering | Audit loss | SHA-256 hash chain + `/verify` |
| Duplicate events | Inflated counts | Idempotency keys (header/body) |
| Partial writes / crash | Lost or orphan records | WAL staging + `recover_uncommitted()` |
| Schema-breaking transforms | Downstream corruption | Compliance agent + quarantine |
| PII in payloads | Privacy exposure | Forbidden-field quarantine (`ssn`, `raw_pan`, `password`) |
| Secret leakage in repo | Credential exposure | CI + G7 secret/provenance scans |

## Out of scope (v0.2)

- Multi-tenant authN/authZ
- Cross-region replication
- Encrypted-at-rest database files
