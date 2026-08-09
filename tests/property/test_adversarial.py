import json
import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from lineage_vault.storage.sqlite_backend import SQLiteStorageBackend


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=30, unique=True))
@settings(max_examples=25, deadline=None)
def test_idempotency_never_double_counts(seqs: list[int]):
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        for i in seqs:
            key = f"k-{i}"
            s.acknowledge_write(idempotency_key=key, event_id=f"e-{i}", payload={"i": i})
            dup = s.acknowledge_write(idempotency_key=key, event_id=f"e-dup-{i}", payload={"i": i})
            assert dup.duplicate
        assert s.count_acknowledged() == len(seqs)
        assert s.verify_integrity()
        s.close()


@given(st.data())
@settings(max_examples=15, deadline=None)
def test_tampered_ledger_fails_integrity(data):
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        n = data.draw(st.integers(min_value=1, max_value=20))
        for i in range(n):
            s.acknowledge_write(idempotency_key=f"k{i}", event_id=f"e{i}", payload={"i": i})
        assert s.verify_integrity()
        s._conn.execute("UPDATE ledger SET entry_hash='0'*64 WHERE seq=1")
        assert not s.verify_integrity()
        s.close()


def test_reordered_recovery_preserves_exact_event_ids():
    with tempfile.TemporaryDirectory() as d:
        s = SQLiteStorageBackend(Path(d) / "v.db")
        staged_ids: list[str] = []
        ack_ids: list[str] = []
        for i in range(10):
            eid = f"e-{i}"
            s.stage_partial(eid, {"order": i})
            staged_ids.append(eid)
        for eid in reversed(staged_ids[:5]):
            s.acknowledge_write(
                idempotency_key=eid,
                event_id=eid,
                payload={"order": int(eid.split("-")[1])},
            )
            ack_ids.append(eid)

        s.recover_uncommitted()
        all_ids = {e.event_id for e in s.all_ledger_entries()}
        assert all_ids == set(staged_ids)
        assert len(all_ids) == 10
        assert s.verify_integrity()
        s.close()
