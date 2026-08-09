from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import uuid4

from ..storage.interface import BatchWriteItem, StorageBackend
from .models import OpenLineageRunEvent


class OpenLineageIngestor:
    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    def ingest(self, event: OpenLineageRunEvent, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.ingest_batch([event], idempotency_keys=[idempotency_key] if idempotency_key else None)[0]

    def ingest_batch(
        self,
        events: list[OpenLineageRunEvent],
        *,
        idempotency_keys: list[str | None] | None = None,
    ) -> list[dict[str, Any]]:
        if not events:
            return []
        keys = idempotency_keys or [None] * len(events)
        items: list[BatchWriteItem] = []
        meta: list[tuple[OpenLineageRunEvent, str]] = []
        for event, key in zip(events, keys, strict=True):
            idem = key or f"{event.run_id}:{event.eventType}"
            event_id = str(uuid4())
            payload = event.model_dump(mode="json")
            items.append(BatchWriteItem(idempotency_key=idem, event_id=event_id, payload=payload))
            meta.append((event, event_id))
        results = self._storage.acknowledge_writes_batch(items)
        out: list[dict[str, Any]] = []
        for (event, _), result in zip(meta, results, strict=True):
            if not result.duplicate:
                self._storage.store_run_event(
                    event.run_id, event.eventType, event.model_dump(mode="json")
                )
                if event.eventType == "COMPLETE":
                    self._record_lineage(event)
            out.append(
                {
                    "event_id": result.event_id,
                    "duplicate": result.duplicate,
                    "acknowledged": result.acknowledged,
                    "run_id": event.run_id,
                    "event_type": event.eventType,
                }
            )
        return out

    def _record_lineage(self, event: OpenLineageRunEvent) -> None:
        ts = event.eventTime
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        inputs = event.datasets_in()
        outputs = event.datasets_out()
        if not inputs or not outputs:
            return
        for out_ds in outputs:
            for in_ds in inputs:
                self._storage.add_dataset_edge(
                    src=in_ds.qualified_name,
                    dst=out_ds.qualified_name,
                    transform_id=event.run_id,
                    event_time=ts,
                    schema_version=len(out_ds.fields),
                    payload=event.model_dump(mode="json"),
                )
                in_fields = {f.name: f.type for f in in_ds.fields}
                out_fields = {f.name: f.type for f in out_ds.fields}
                if in_fields and out_fields:
                    field_map = {k: k for k in out_fields if k in in_fields}
                    if field_map:
                        self._storage.record_field_mapping(
                            run_id=event.run_id,
                            output_dataset=out_ds.qualified_name,
                            input_dataset=in_ds.qualified_name,
                            field_map=field_map,
                        )
        for out_ds, in_ds, run_id, field_map in event.field_mappings():
            self._storage.record_field_mapping(
                run_id=run_id,
                output_dataset=out_ds,
                input_dataset=in_ds,
                field_map=field_map,
            )
