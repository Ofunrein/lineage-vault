from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StorageConfig:
    backend: str
    data_dir: Path
    sqlite_path: Path
    postgres_dsn: str | None = None
    postgres_pool_min: int = 1
    postgres_pool_max: int = 10
    max_batch_size: int = 500
    max_request_bytes: int = 1_048_576


def load_storage_config(*, data_dir: str | Path = ".data") -> StorageConfig:
    root = Path(data_dir)
    backend = os.environ.get("LINEAGE_VAULT_BACKEND", "sqlite").strip().lower()
    postgres_dsn = os.environ.get("LINEAGE_VAULT_POSTGRES_DSN") or os.environ.get("DATABASE_URL")
    sqlite_path = Path(
        os.environ.get("LINEAGE_VAULT_SQLITE_PATH", str(root / "vault.db"))
    )
    max_batch = int(os.environ.get("LINEAGE_VAULT_MAX_BATCH_SIZE", "500"))
    max_bytes = int(os.environ.get("LINEAGE_VAULT_MAX_REQUEST_BYTES", "1048576"))
    pool_min = int(os.environ.get("LINEAGE_VAULT_PG_POOL_MIN", "1"))
    pool_max = int(os.environ.get("LINEAGE_VAULT_PG_POOL_MAX", "10"))
    return StorageConfig(
        backend=backend,
        data_dir=root,
        sqlite_path=sqlite_path,
        postgres_dsn=postgres_dsn,
        postgres_pool_min=pool_min,
        postgres_pool_max=pool_max,
        max_batch_size=max_batch,
        max_request_bytes=max_bytes,
    )
