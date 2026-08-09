"""PostgreSQL concurrency and integrity tests (require LINEAGE_VAULT_POSTGRES_DSN)."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import psycopg
import pytest

from lineage_vault.storage.config import StorageConfig
from lineage_vault.storage.factory import create_storage_backend, postgres_available
from lineage_vault.storage.interface import BatchWriteItem
from lineage_vault.storage.postgres_backend import PostgresStorageBackend

_TABLES = (
    "field_mappings",
    "dataset_edges",
    "wal_staging",
    "ledger",
    "idempotency",
    "run_events",
    "schema_migrations",
)


def _postgres_dsn() -> str | None:
    return os.environ.get("LINEAGE_VAULT_POSTGRES_DSN") or os.environ.get("DATABASE_URL")


def _truncate_postgres(dsn: str) -> None:
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "TRUNCATE TABLE "
                + ", ".join(_TABLES)
                + " RESTART IDENTITY CASCADE"
            )
    except psycopg.Error:
        return


def _require_postgres() -> str:
    dsn = _postgres_dsn()
    if not dsn or not postgres_available(dsn):
        pytest.skip("postgres unavailable")
    return dsn


def _backend(dsn: str) -> PostgresStorageBackend:
    config = StorageConfig(
        backend="postgres",
        data_dir=os.getcwd(),
        sqlite_path=os.getcwd(),
        postgres_dsn=dsn,
        postgres_pool_min=2,
        postgres_pool_max=8,
    )
    backend = create_storage_backend(config)
    assert isinstance(backend, PostgresStorageBackend)
    _truncate_postgres(dsn)
    return backend


def test_postgres_pool_bounded_and_concurrent_batches_preserve_integrity():
    dsn = _require_postgres()
    backend = _backend(dsn)
    assert backend.pool_size == 8

    total = 400
    batch_size = 25
    workers = 4

    def worker(batch_start: int) -> int:
        items = [
            BatchWriteItem(
                idempotency_key=f"pg:{batch_start + i}",
                event_id=str(uuid4()),
                payload={"i": batch_start + i},
            )
            for i in range(batch_size)
        ]
        results = backend.acknowledge_writes_batch(items)
        return sum(1 for r in results if r.acknowledged and not r.duplicate)

    starts = list(range(0, total, batch_size))
    acked = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, s) for s in starts]
        for fut in as_completed(futures):
            acked += fut.result()

    assert acked == total
    assert backend.count_acknowledged() == total
    assert backend.verify_integrity()
    backend.close()
    _truncate_postgres(dsn)


def test_postgres_duplicate_batch_does_not_grow_ledger():
    dsn = _require_postgres()
    backend = _backend(dsn)
    item = BatchWriteItem(idempotency_key="dup", event_id="e1", payload={"v": 1})
    r1 = backend.acknowledge_writes_batch([item])
    r2 = backend.acknowledge_writes_batch([item])
    assert r1[0].duplicate is False
    assert r2[0].duplicate is True
    assert backend.count_acknowledged() == 1
    assert backend.verify_integrity()
    backend.close()
    _truncate_postgres(dsn)
