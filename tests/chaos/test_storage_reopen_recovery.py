"""Crash/reopen recovery contract tests — see docs/crash-semantics.md."""

from __future__ import annotations

import tempfile
from pathlib import Path

from lineage_vault.storage.sqlite_backend import SQLiteStorageBackend


def test_close_reopen_acknowledged_survive_without_recovery():
    """Acknowledged writes must survive DB close/reopen with exact IDs and idempotency."""
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "vault.db"
        ack_specs: list[tuple[str, str, dict]] = []

        s1 = SQLiteStorageBackend(db_path)
        for i in range(10):
            eid = f"ack-{i}"
            key = f"idem-{i}"
            payload = {"seq": i, "kind": "acknowledged"}
            s1.acknowledge_write(idempotency_key=key, event_id=eid, payload=payload)
            ack_specs.append((key, eid, payload))
        s1.close()

        s2 = SQLiteStorageBackend(db_path)
        entries = s2.all_ledger_entries()
        assert len(entries) == 10
        assert {e.event_id for e in entries} == {eid for _, eid, _ in ack_specs}
        assert s2.verify_integrity()

        for key, eid, payload in ack_specs:
            dup = s2.acknowledge_write(
                idempotency_key=key,
                event_id="should-not-append",
                payload={"tamper": True},
            )
            assert dup.duplicate is True
            assert dup.event_id == eid

        by_id = {e.event_id: e.payload for e in s2.all_ledger_entries()}
        for _, eid, payload in ack_specs:
            assert by_id[eid] == payload

        assert s2.recover_uncommitted() == 0
        assert len(s2.all_ledger_entries()) == 10
        s2.close()


def test_close_reopen_staged_recovery_exact_ids_payloads_and_chain():
    """Staged writes may complete on recovery; unstaged events must never appear."""
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "vault.db"
        ack_ids: list[str] = []
        staged_ids: list[str] = []
        staged_payloads: dict[str, dict] = {}
        idem_map: dict[str, str] = {}

        s1 = SQLiteStorageBackend(db_path)
        for i in range(20):
            eid = f"ev-{i}"
            if i % 2 == 0:
                key = f"idem-{i}"
                payload = {"seq": i, "kind": "acknowledged"}
                s1.acknowledge_write(idempotency_key=key, event_id=eid, payload=payload)
                ack_ids.append(eid)
                idem_map[key] = eid
            else:
                payload = {"seq": i, "kind": "staged-only"}
                s1.stage_partial(eid, payload)
                staged_ids.append(eid)
                staged_payloads[eid] = payload

        unstaged_id = "ev-never-written"
        s1.close()

        s2 = SQLiteStorageBackend(db_path)

        pre_entries = s2.all_ledger_entries()
        assert {e.event_id for e in pre_entries} == set(ack_ids)
        assert unstaged_id not in {e.event_id for e in pre_entries}
        assert s2.verify_integrity()

        for key, expected_eid in idem_map.items():
            dup = s2.acknowledge_write(
                idempotency_key=key,
                event_id="ev-replay-attempt",
                payload={"forged": True},
            )
            assert dup.duplicate is True
            assert dup.event_id == expected_eid

        recovered = s2.recover_uncommitted()
        assert recovered == len(staged_ids)

        all_entries = s2.all_ledger_entries()
        all_ids = {e.event_id for e in all_entries}
        assert all_ids == set(ack_ids) | set(staged_ids)
        assert unstaged_id not in all_ids
        assert len(all_entries) == len(ack_ids) + len(staged_ids)

        by_id = {e.event_id: e.payload for e in all_entries}
        for eid in ack_ids:
            seq = int(eid.split("-")[1])
            assert by_id[eid] == {"seq": seq, "kind": "acknowledged"}
        for eid in staged_ids:
            assert by_id[eid] == staged_payloads[eid]

        assert s2.verify_integrity()
        assert s2.recover_uncommitted() == 0
        assert len(s2.all_ledger_entries()) == len(all_entries)
        s2.close()


def test_recovery_never_invents_unstaged_events():
    """Only durably staged event IDs may appear after recovery."""
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "vault.db"
        s1 = SQLiteStorageBackend(db_path)
        s1.stage_partial("only-staged", {"marker": 1})
        s1.close()

        s2 = SQLiteStorageBackend(db_path)
        assert s2.recover_uncommitted() == 1
        ids = {e.event_id for e in s2.all_ledger_entries()}
        assert ids == {"only-staged"}
        assert s2.verify_integrity()
        s2.close()

        s3 = SQLiteStorageBackend(db_path)
        assert s3.recover_uncommitted() == 0
        assert {e.event_id for e in s3.all_ledger_entries()} == {"only-staged"}
        s3.close()
