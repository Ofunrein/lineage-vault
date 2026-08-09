"""Duplicate batch ingestion must not expand graph or run_events."""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from lineage_vault.engine import LineageVaultEngine
from lineage_vault.openlineage.ingest import OpenLineageIngestor
from lineage_vault.openlineage.models import OpenLineageRunEvent


def _complete_event(run_id: str) -> OpenLineageRunEvent:
    return OpenLineageRunEvent(
        eventType="COMPLETE",
        eventTime=datetime(2026, 1, 1, tzinfo=UTC),
        run={"runId": run_id},
        job={"namespace": "dup", "name": "test"},
        inputs=[
            {
                "namespace": "dup",
                "name": "src",
                "facets": {"schema": {"fields": [{"name": "id", "type": "int"}]}},
            }
        ],
        outputs=[
            {
                "namespace": "dup",
                "name": "dst",
                "facets": {"schema": {"fields": [{"name": "id", "type": "int"}]}},
            }
        ],
    )


def _table_count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def test_duplicate_batch_skips_graph_and_run_event_writes():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        eng = LineageVaultEngine(root)
        ingestor = OpenLineageIngestor(eng.storage)
        ev = _complete_event("run-dup")
        db_path = eng.config.sqlite_path

        first = ingestor.ingest_batch([ev], idempotency_keys=["idem-1"])
        assert first[0]["duplicate"] is False
        edge_first = _table_count(db_path, "dataset_edges")
        run_first = _table_count(db_path, "run_events")
        assert edge_first >= 1
        assert run_first == 1

        second = ingestor.ingest_batch([ev], idempotency_keys=["idem-1"])
        assert second[0]["duplicate"] is True
        assert _table_count(db_path, "dataset_edges") == edge_first
        assert _table_count(db_path, "run_events") == run_first
        assert eng.storage.count_acknowledged() == 1
        eng.close()
