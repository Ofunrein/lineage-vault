"""Gauntlet L2 — time-travel == replay."""
import tempfile
from datetime import datetime, timezone, timedelta
from lineage_vault.engine import LineageVaultEngine
from lineage_vault.models.events import LineageEvent, SchemaVersion, TransformRecord

def test_gauntlet_l2_snapshot_matches_replay():
    with tempfile.TemporaryDirectory() as d:
        eng = LineageVaultEngine(d)
        t0 = datetime.now(timezone.utc)
        eng.ingest_sync(LineageEvent(
            pipeline_run_id="r", sequence=1,
            transform=TransformRecord(
                input_dataset="raw", output_dataset="mart",
                input_schema=SchemaVersion(version=1, fields={"id": "int"}),
                output_schema=SchemaVersion(version=2, fields={"id": "int"}),
            ),
        ))
        at = t0 + timedelta(seconds=1)
        snap = eng.timetravel.snapshot("mart", at)
        replay = eng.timetravel.replay_at("mart", at)
        assert snap["dataset_id"] == replay["dataset_id"]
        assert eng.verify()
        eng.close()
