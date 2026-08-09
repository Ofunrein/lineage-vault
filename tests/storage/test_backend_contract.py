"""Shared storage backend contract tests (sqlite + postgres when available)."""

from __future__ import annotations

from datetime import UTC, datetime

from lineage_vault.storage.interface import BatchWriteItem, StorageBackend


def test_batch_idempotency_and_integrity(storage_backend: StorageBackend):
    items = [
        BatchWriteItem(idempotency_key=f"k{i}", event_id=f"e{i}", payload={"n": i})
        for i in range(100)
    ]
    first = storage_backend.acknowledge_writes_batch(items)
    assert len(first) == 100
    assert all(r.duplicate is False for r in first)
    second = storage_backend.acknowledge_writes_batch(items)
    assert all(r.duplicate is True for r in second)
    assert storage_backend.count_acknowledged() == 100
    assert storage_backend.verify_integrity()


def test_batch_preserves_acknowledged_count_under_mixed_duplicates(storage_backend: StorageBackend):
    base = [
        BatchWriteItem(idempotency_key=f"mix-{i}", event_id=f"ev-{i}", payload={"i": i})
        for i in range(50)
    ]
    storage_backend.acknowledge_writes_batch(base)
    # replay half duplicates + half new
    mixed = base[:25] + [
        BatchWriteItem(idempotency_key=f"new-{i}", event_id=f"new-ev-{i}", payload={"i": i})
        for i in range(25)
    ]
    storage_backend.acknowledge_writes_batch(mixed)
    assert storage_backend.count_acknowledged() == 75
    assert storage_backend.verify_integrity()


def test_field_impact_on_backend(storage_backend: StorageBackend):
    now = datetime.now(UTC)
    storage_backend.add_dataset_edge(
        src="a.raw",
        dst="a.curated",
        transform_id="t1",
        event_time=now,
        schema_version=1,
        payload={},
    )
    storage_backend.record_field_mapping(
        run_id="r1",
        output_dataset="a.curated",
        input_dataset="a.raw",
        field_map={"amount_usd": "amount"},
    )
    impact = storage_backend.field_impact("a.raw", "amount")
    assert ("a.curated", "amount_usd") in {
        (x["dataset"], x["field"]) for x in impact["impacted_fields"]
    }
