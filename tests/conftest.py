from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest

from lineage_vault.storage.config import StorageConfig
from lineage_vault.storage.factory import create_storage_backend, postgres_available
from lineage_vault.storage.interface import StorageBackend
from lineage_vault.storage.sqlite_backend import SQLiteStorageBackend

TABLES = (
    "field_mappings",
    "dataset_edges",
    "wal_staging",
    "ledger",
    "idempotency",
    "run_events",
    "schema_migrations",
)


def postgres_dsn() -> str | None:
    return os.environ.get("LINEAGE_VAULT_POSTGRES_DSN") or os.environ.get("DATABASE_URL")


def postgres_is_available() -> bool:
    dsn = postgres_dsn()
    return bool(dsn and postgres_available(dsn))


def _truncate_postgres(dsn: str) -> None:
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "TRUNCATE TABLE "
                + ", ".join(TABLES)
                + " RESTART IDENTITY CASCADE"
            )
    except psycopg.Error:
        return


@pytest.fixture
def sqlite_backend(tmp_path) -> Iterator[StorageBackend]:
    backend = SQLiteStorageBackend(tmp_path / "vault.db")
    yield backend
    backend.close()


@pytest.fixture
def postgres_backend() -> Iterator[StorageBackend]:
    dsn = postgres_dsn()
    if not dsn or not postgres_available(dsn):
        pytest.skip("postgres unavailable")
    config = StorageConfig(
        backend="postgres",
        data_dir=os.getcwd(),
        sqlite_path=os.getcwd(),
        postgres_dsn=dsn,
    )
    backend = create_storage_backend(config)
    _truncate_postgres(dsn)
    yield backend
    backend.close()
    _truncate_postgres(dsn)


@pytest.fixture
def storage_backend(request, tmp_path) -> Iterator[StorageBackend]:
    """Parametrized backend: sqlite always runs; postgres skips only when DSN unavailable."""
    backend_name = request.param
    if backend_name == "sqlite":
        backend = SQLiteStorageBackend(tmp_path / "vault.db")
        yield backend
        backend.close()
        return
    if backend_name == "postgres":
        dsn = postgres_dsn()
        if not dsn or not postgres_available(dsn):
            pytest.skip("postgres unavailable")
        config = StorageConfig(
            backend="postgres",
            data_dir=tmp_path,
            sqlite_path=tmp_path / "vault.db",
            postgres_dsn=dsn,
        )
        backend = create_storage_backend(config)
        _truncate_postgres(dsn)
        yield backend
        backend.close()
        _truncate_postgres(dsn)
        return
    pytest.fail(f"unknown storage backend param: {backend_name}")


def pytest_generate_tests(metafunc):
    if "storage_backend" in metafunc.fixturenames:
        metafunc.parametrize("storage_backend", ["sqlite", "postgres"], indirect=True)
