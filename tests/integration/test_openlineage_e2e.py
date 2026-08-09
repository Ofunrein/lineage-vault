import tempfile
from datetime import UTC, datetime

from lineage_vault.engine import LineageVaultEngine
from lineage_vault.openlineage.models import OpenLineageRunEvent


def test_openlineage_complete_ingest_and_impact():
    with tempfile.TemporaryDirectory() as d:
        eng = LineageVaultEngine(d)
        run_id = "run-ol-1"
        now = datetime.now(UTC)
        complete = OpenLineageRunEvent(
            eventType="COMPLETE",
            eventTime=now,
            run={"runId": run_id},
            job={"namespace": "demo", "name": "etl"},
            inputs=[
                {
                    "namespace": "warehouse",
                    "name": "raw_orders",
                    "facets": {
                        "schema": {"fields": [{"name": "amount", "type": "double"}]}
                    },
                }
            ],
            outputs=[
                {
                    "namespace": "warehouse",
                    "name": "curated_orders",
                    "facets": {
                        "schema": {"fields": [{"name": "amount_usd", "type": "double"}]},
                        "columnLineage": {
                            "fields": {
                                "amount_usd": {
                                    "inputFields": [
                                        {
                                            "namespace": "warehouse",
                                            "name": "raw_orders",
                                            "field": "amount",
                                        }
                                    ]
                                }
                            }
                        },
                    },
                }
            ],
        )
        r1 = eng.openlineage.ingest(complete, idempotency_key=f"{run_id}:COMPLETE")
        r2 = eng.openlineage.ingest(complete, idempotency_key=f"{run_id}:COMPLETE")
        assert r1["duplicate"] is False
        assert r2["duplicate"] is True
        impact = eng.impact.field_impact("warehouse.raw_orders", "amount")
        assert any(
            f["dataset"] == "warehouse.curated_orders" and f["field"] == "amount_usd"
            for f in impact["impacted_fields"]
        )
        assert eng.verify()
        eng.close()
