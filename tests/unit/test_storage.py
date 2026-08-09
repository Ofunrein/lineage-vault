"""Storage backend tests."""

import tempfile
from datetime import UTC
from pathlib import Path

from lineage_vault.storage.sqlite_backend import SQLiteStorageBackend


def test_migrations_applied():
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        assert s.migrate() == 0
        s.close()


def test_idempotent_acknowledged_writes():
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        r1 = s.acknowledge_write(idempotency_key="k1", event_id="e1", payload={"n": 1})
        r2 = s.acknowledge_write(idempotency_key="k1", event_id="e2", payload={"n": 2})
        assert r1.duplicate is False
        assert r2.duplicate is True
        assert s.count_acknowledged() == 1
        assert s.verify_integrity()
        s.close()


def test_crash_recovery_staged_completion_contract():
    """Acknowledged writes survive; staged writes complete on recovery; counts are exact."""
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "v.db"
        s = SQLiteStorageBackend(db_path)
        ack_ids: list[str] = []
        staged_ids: list[str] = []
        for i in range(50):
            eid = f"ev-{i}"
            if i % 2 == 0:
                s.acknowledge_write(
                    idempotency_key=f"idem-{i}",
                    event_id=eid,
                    payload={"seq": i, "kind": "ack"},
                )
                ack_ids.append(eid)
            else:
                s.stage_partial(eid, {"seq": i, "kind": "staged"})
                staged_ids.append(eid)

        assert {e.event_id for e in s.all_ledger_entries()} == set(ack_ids)
        recovered = s.recover_uncommitted()
        assert recovered == len(staged_ids)
        all_ids = {e.event_id for e in s.all_ledger_entries()}
        assert all_ids == set(ack_ids) | set(staged_ids)
        assert len(all_ids) == 50
        assert s.verify_integrity()
        s.close()


def test_tamper_detection():
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        s.acknowledge_write(idempotency_key="k", event_id="e", payload={"v": 1})
        assert s.verify_integrity()
        s._conn.execute("UPDATE ledger SET payload='{\"hack\":true}' WHERE event_id='e'")
        assert not s.verify_integrity()
        s.close()


def test_field_impact_traversal():
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        from datetime import datetime

        now = datetime.now(UTC)
        s.add_dataset_edge(
            src="a.raw", dst="a.curated", transform_id="t1",
            event_time=now, schema_version=1, payload={},
        )
        s.add_dataset_edge(
            src="a.curated", dst="a.mart", transform_id="t2",
            event_time=now, schema_version=1, payload={},
        )
        s.record_field_mapping(
            run_id="r1", output_dataset="a.curated", input_dataset="a.raw",
            field_map={"amount_usd": "amount"},
        )
        s.record_field_mapping(
            run_id="r2", output_dataset="a.mart", input_dataset="a.curated",
            field_map={"total_usd": "amount_usd"},
        )
        impact = s.field_impact("a.raw", "amount")
        fields = {(x["dataset"], x["field"]) for x in impact["impacted_fields"]}
        assert ("a.curated", "amount_usd") in fields
        assert ("a.mart", "total_usd") in fields
        assert "a.mart" in impact["downstream_datasets"]
        s.close()
