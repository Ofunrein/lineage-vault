from __future__ import annotations

from .config import StorageConfig
from .interface import StorageBackend
from .sqlite_backend import SQLiteStorageBackend


def create_storage_backend(config: StorageConfig) -> StorageBackend:
    if config.backend == "postgres":
        if not config.postgres_dsn:
            raise ValueError("LINEAGE_VAULT_BACKEND=postgres requires LINEAGE_VAULT_POSTGRES_DSN")
        from .postgres_backend import PostgresStorageBackend

        return PostgresStorageBackend(
            config.postgres_dsn,
            pool_min=config.postgres_pool_min,
            pool_max=config.postgres_pool_max,
        )
    if config.backend != "sqlite":
        raise ValueError(f"unsupported storage backend: {config.backend}")
    config.data_dir.mkdir(parents=True, exist_ok=True)
    return SQLiteStorageBackend(config.sqlite_path)


def postgres_available(dsn: str) -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        return True
    except (OSError, psycopg.Error):
        return False
