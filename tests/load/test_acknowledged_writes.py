"""Load tests — concurrent ingestion with zero lost acknowledged writes."""

from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from uuid import uuid4

from lineage_vault.engine import LineageVaultEngine
from lineage_vault.openlineage.models import OpenLineageRunEvent
from lineage_vault.storage.interface import BatchWriteItem


def _event(i: int) -> OpenLineageRunEvent:
    return OpenLineageRunEvent(
        eventType="COMPLETE",
        eventTime=datetime.now(UTC),
        run={"runId": f"load-{i}"},
        job={"namespace": "load", "name": "test"},
        inputs=[{"namespace": "load", "name": f"in-{i}"}],
        outputs=[{"namespace": "load", "name": f"out-{i}"}],
    )


def test_concurrent_single_ingest_zero_lost_acknowledgements():
    events = 400
    workers = 8
    with tempfile.TemporaryDirectory() as d:
        eng = LineageVaultEngine(d)
        eng.recover()

        def worker(i: int) -> None:
            eng.openlineage.ingest(_event(i), idempotency_key=f"load:{i}")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(worker, range(events)))

        assert eng.storage.count_acknowledged() == events
        assert eng.verify()
        eng.close()


def test_concurrent_batch_ingest_zero_lost_acknowledgements():
    events = 400
    batch_size = 25
    workers = 4
    with tempfile.TemporaryDirectory() as d:
        eng = LineageVaultEngine(d)
        eng.recover()

        def worker(batch_start: int) -> int:
            items = [
                BatchWriteItem(
                    idempotency_key=f"b:{batch_start + offset}",
                    event_id=str(uuid4()),
                    payload=_event(batch_start + offset).model_dump(mode="json"),
                )
                for offset in range(batch_size)
            ]
            results = eng.storage.acknowledge_writes_batch(items)
            return sum(1 for r in results if r.acknowledged and not r.duplicate)

        starts = list(range(0, events, batch_size))
        acked = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, s) for s in starts]
            for fut in as_completed(futures):
                acked += fut.result()

        assert acked == events
        assert eng.storage.count_acknowledged() == events
        assert eng.verify()
        eng.close()
