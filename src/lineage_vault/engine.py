from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from .compliance.agent import ComplianceAgent
from .models.events import LineageEvent
from .openlineage.impact import ImpactAnalyzer
from .openlineage.ingest import OpenLineageIngestor
from .schema.engine import SchemaEngine
from .storage.sqlite_backend import SQLiteStorageBackend
from .timetravel.query import TimeTravelEngine

logger = logging.getLogger("lineage_vault.engine")


class _GraphFacade:
    def __init__(self, storage: SQLiteStorageBackend) -> None:
        self._storage = storage

    def upstream(self, dataset_id: str) -> list[str]:
        return self._storage.upstream_datasets(dataset_id)

    def downstream(self, dataset_id: str) -> list[str]:
        return self._storage.downstream_datasets(dataset_id)

    def edges_at(self, at: datetime) -> list[dict[str, Any]]:
        return self._storage.edges_at(at)

    def add_edge(
        self,
        src: str,
        dst: str,
        transform_id: str,
        ts: datetime,
        schema_version: int,
        payload: dict[str, Any],
    ) -> None:
        self._storage.add_dataset_edge(
            src=src,
            dst=dst,
            transform_id=transform_id,
            event_time=ts,
            schema_version=schema_version,
            payload=payload,
        )

    def close(self) -> None:
        return None


class _LedgerFacade:
    def __init__(self, storage: SQLiteStorageBackend) -> None:
        self._storage = storage

    def verify_integrity(self) -> bool:
        return self._storage.verify_integrity()

    def all_entries(self):
        return self._storage.all_ledger_entries()

    def append(self, event_id: str, payload: dict[str, Any]):
        return self._storage.acknowledge_write(
            idempotency_key=event_id,
            event_id=event_id,
            payload=payload,
        )

    def close(self) -> None:
        return None


class _WalFacade:
    def __init__(self, storage: SQLiteStorageBackend) -> None:
        self._storage = storage

    def stage(self, event_id: str, payload: dict[str, Any]) -> None:
        self._storage.stage_partial(event_id, payload)

    def commit(self, event_id: str, ledger_append) -> None:
        self._storage.acknowledge_write(
            idempotency_key=event_id,
            event_id=event_id,
            payload=ledger_append if isinstance(ledger_append, dict) else {},
        )

    def replay_pending(self, ledger_append) -> int:
        return self._storage.recover_uncommitted()

    def close(self) -> None:
        return None


class LineageVaultEngine:
    def __init__(self, data_dir: str | Path = ".data") -> None:
        d = Path(data_dir)
        d.mkdir(parents=True, exist_ok=True)
        self.storage = SQLiteStorageBackend(d / "vault.db")
        self.graph = _GraphFacade(self.storage)
        self.ledger = _LedgerFacade(self.storage)
        self.wal = _WalFacade(self.storage)
        self.timetravel = TimeTravelEngine(self.graph, self.ledger)
        self.schema = SchemaEngine()
        self.compliance = ComplianceAgent()
        self.openlineage = OpenLineageIngestor(self.storage)
        self.impact = ImpactAnalyzer(self.storage)

    def _persist_event(self, event: LineageEvent, *, idempotency_key: str | None = None) -> None:
        payload = event.model_dump(mode="json")
        payload["output_dataset"] = event.transform.output_dataset
        payload["input_dataset"] = event.transform.input_dataset
        payload["timestamp"] = event.timestamp.isoformat()
        key = idempotency_key or event.event_id
        self.storage.acknowledge_write(
            idempotency_key=key,
            event_id=event.event_id,
            payload=payload,
        )
        status, compat = self.compliance.check(event)
        event.transform.compliance = status
        event.transform.compat = compat
        self.graph.add_edge(
            event.transform.input_dataset,
            event.transform.output_dataset,
            event.transform.transform_id,
            event.timestamp,
            event.transform.output_schema.version,
            payload,
        )
        logger.info("event persisted", extra={"event_id": event.event_id})

    def ingest_sync(self, event: LineageEvent, *, idempotency_key: str | None = None) -> None:
        self._persist_event(event, idempotency_key=idempotency_key)

    def recover(self) -> int:
        return self.storage.recover_uncommitted()

    def verify(self) -> bool:
        return self.storage.verify_integrity()

    def close(self) -> None:
        self.storage.close()
