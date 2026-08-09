"""Failure injection — acknowledged writes survive close/reopen and recovery."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from lineage_vault.storage.interface import BatchWriteItem
from lineage_vault.storage.sqlite_backend import SQLiteStorageBackend


def test_batch_acknowledged_survive_close_reopen():
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "vault.db"
        items = [
            BatchWriteItem(idempotency_key=f"k{i}", event_id=f"e{i}", payload={"seq": i})
            for i in range(200)
        ]
        s1 = SQLiteStorageBackend(db_path)
        results = s1.acknowledge_writes_batch(items)
        assert all(r.acknowledged for r in results)
        assert s1.count_acknowledged() == 200
        s1.close()

        s2 = SQLiteStorageBackend(db_path)
        assert s2.count_acknowledged() == 200
        assert {e.event_id for e in s2.all_ledger_entries()} == {f"e{i}" for i in range(200)}
        assert s2.verify_integrity()
        assert s2.recover_uncommitted() == 0
        s2.close()


def test_injected_failure_after_partial_batch_leaves_no_phantom_ack():
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "vault.db"
        s1 = SQLiteStorageBackend(db_path)
        ack = s1.acknowledge_writes_batch(
            [BatchWriteItem(idempotency_key="a1", event_id="e1", payload={"v": 1})]
        )
        assert ack[0].duplicate is False
        s1.stage_partial("staged-1", {"kind": "staged"})
        s1.close()

        s2 = SQLiteStorageBackend(db_path)
        assert s2.count_acknowledged() == 1
        assert s2.recover_uncommitted() == 1
        assert s2.count_acknowledged() == 2
        assert "phantom" not in {e.event_id for e in s2.all_ledger_entries()}
        s2.close()


@pytest.mark.parametrize("batch_size", [1, 10, 50])
def test_zero_lost_acknowledged_writes_under_batch_sizes(batch_size: int):
    total = 500
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "vault.db")
        for start in range(0, total, batch_size):
            chunk = [
                BatchWriteItem(
                    idempotency_key=f"load-{i}",
                    event_id=f"ev-{i}",
                    payload={"i": i},
                )
                for i in range(start, min(start + batch_size, total))
            ]
            results = s.acknowledge_writes_batch(chunk)
            assert all(r.acknowledged for r in results)
        assert s.count_acknowledged() == total
        assert s.verify_integrity()
        s.close()
