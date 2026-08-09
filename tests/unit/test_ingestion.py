"""Gauntlet L3 — burst ingestion bounded."""
import asyncio
import tempfile

from lineage_vault.engine import LineageVaultEngine
from lineage_vault.ingestion.pipeline import IngestionPipeline
from lineage_vault.models.events import LineageEvent, SchemaVersion, TransformRecord


async def _run_burst(n: int = 500, max_q: int = 200):
    with tempfile.TemporaryDirectory() as d:
        eng = LineageVaultEngine(d)
        async def handler(ev: LineageEvent) -> None:
            eng.ingest_sync(ev)
        pipe = IngestionPipeline(handler, max_queue=max_q, rate=100000)
        await pipe.start(4)
        accepted = 0
        for i in range(n):
            ev = LineageEvent(
                pipeline_run_id="b", sequence=i,
                transform=TransformRecord(
                    input_dataset=f"a{i%10}", output_dataset=f"b{i%10}",
                    input_schema=SchemaVersion(version=1, fields={"x": "int"}),
                    output_schema=SchemaVersion(version=2, fields={"x": "int"}),
                ),
            )
            if await pipe.ingest(ev):
                accepted += 1
        await pipe._queue.join()
        await pipe.stop()
        assert pipe.max_queue == max_q
        assert accepted + pipe.dropped == n
        assert eng.verify()
        eng.close()

def test_gauntlet_l3_burst_bounded():
    asyncio.run(_run_burst())
