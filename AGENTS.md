# LineageVault — MoT + Gauntlet

## MoT MLR Graph
M1 models → M2 ledger → M3 wal → M4 graph → M5 timetravel → M6 schema → M7 compliance → M8 ingestion → M9 lifecycle → M10 api

## Gauntlet Loops
| Loop | Module | Critic bar | Test |
|------|--------|------------|------|
| L1 | ledger+wal | crash replay, verify_integrity | tests/chaos/test_crash_recovery.py |
| L2 | graph+timetravel | snapshot == replay | tests/unit/test_timetravel.py |
| L3 | ingestion | burst bounded queue | tests/unit/test_ingestion.py |
| L4 | schema+compliance | 20 fixtures 100% | tests/fixtures/test_schema_fixtures.py |
| L5 | api | ≤3 calls analyst query | tests/integration/test_api.py |
