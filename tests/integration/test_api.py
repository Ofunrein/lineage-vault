"""Gauntlet L5 — analyst query in ≤3 API calls."""
import tempfile
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from lineage_vault.api.app import create_app

def test_gauntlet_l5_analyst_three_calls():
    with tempfile.TemporaryDirectory() as d:
        client = TestClient(create_app(d))
        # Call 1: ingest
        r1 = client.post("/events", json={
            "pipeline_run_id": "incident", "sequence": 1,
            "input_dataset": "raw_orders", "output_dataset": "mart_orders",
            "input_schema": {"id": "int"}, "output_schema": {"id": "int", "region": "str"},
            "row_count": 5000,
        })
        assert r1.status_code == 200
        # Call 2: snapshot at incident time
        at = datetime.now(timezone.utc).isoformat()
        r2 = client.get("/snapshot", params={"dataset_id": "mart_orders", "at": at})
        assert r2.status_code == 200
        assert r2.json()["dataset_id"] == "mart_orders"
        # Call 3: verify integrity
        r3 = client.post("/verify")
        assert r3.status_code == 200
        assert r3.json()["valid"] is True
