# LineageVault

Tamper-proof data lineage + audit ledger for open-source data governance.

## Features
- Hash-chained append-only ledger
- WAL crash recovery
- DAG lineage with time-travel queries
- Schema evolution (forward/backward/breaking)
- Compliance quarantine agent
- Async ingestion with backpressure
- Cold-path lifecycle tiering

## Quick start
```bash
pip install -e ".[dev]"
pytest -v
lineage-vault serve
scripts/run_gauntlet.sh
```
