from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class LedgerEntry:
    seq: int
    event_id: str
    payload: dict[str, Any]
    prev_hash: str
    entry_hash: str


@dataclass(frozen=True)
class AcknowledgedWrite:
    event_id: str
    idempotency_key: str
    acknowledged: bool
    duplicate: bool


class StorageBackend(ABC):
    @abstractmethod
    def migrate(self) -> int: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def acknowledge_write(
        self,
        *,
        idempotency_key: str,
        event_id: str,
        payload: dict[str, Any],
    ) -> AcknowledgedWrite: ...

    @abstractmethod
    def stage_partial(self, event_id: str, payload: dict[str, Any]) -> None: ...

    @abstractmethod
    def recover_uncommitted(self) -> int: ...

    @abstractmethod
    def verify_integrity(self) -> bool: ...

    @abstractmethod
    def count_acknowledged(self) -> int: ...

    @abstractmethod
    def all_ledger_entries(self) -> list[LedgerEntry]: ...

    @abstractmethod
    def add_dataset_edge(
        self,
        *,
        src: str,
        dst: str,
        transform_id: str,
        event_time: datetime,
        schema_version: int,
        payload: dict[str, Any],
    ) -> None: ...

    @abstractmethod
    def upstream_datasets(self, dataset_id: str) -> list[str]: ...

    @abstractmethod
    def downstream_datasets(self, dataset_id: str) -> list[str]: ...

    @abstractmethod
    def edges_at(self, at: datetime) -> list[dict[str, Any]]: ...

    @abstractmethod
    def record_field_mapping(
        self,
        *,
        run_id: str,
        output_dataset: str,
        input_dataset: str,
        field_map: dict[str, str],
    ) -> None: ...

    @abstractmethod
    def field_impact(self, dataset: str, field: str) -> dict[str, Any]: ...

    @abstractmethod
    def store_run_event(self, run_id: str, event_type: str, payload: dict[str, Any]) -> None: ...

    @abstractmethod
    def get_run_event(self, run_id: str, event_type: str) -> dict[str, Any] | None: ...
