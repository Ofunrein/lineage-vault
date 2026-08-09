from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from ..engine import LineageVaultEngine
from ..models.events import LineageEvent, SchemaVersion, TransformRecord
from ..openlineage.models import OpenLineageRunEvent


def run_demo_pipeline(engine: LineageVaultEngine) -> dict:
    """Simulate a realistic ETL pipeline with OpenLineage events."""
    run_id = str(uuid4())
    now = datetime.now(timezone.utc)

    start = OpenLineageRunEvent(
        eventType="START",
        eventTime=now,
        run={"runId": run_id},
        job={"namespace": "demo", "name": "orders_etl"},
        inputs=[
            {
                "namespace": "warehouse",
                "name": "raw_orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "integer"},
                            {"name": "customer_id", "type": "integer"},
                            {"name": "amount", "type": "double"},
                        ]
                    }
                },
            }
        ],
        outputs=[],
    )
    engine.openlineage.ingest(start, idempotency_key=f"{run_id}:START")

    complete = OpenLineageRunEvent(
        eventType="COMPLETE",
        eventTime=now,
        run={"runId": run_id},
        job={"namespace": "demo", "name": "orders_etl"},
        inputs=[
            {
                "namespace": "warehouse",
                "name": "raw_orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "integer"},
                            {"name": "customer_id", "type": "integer"},
                            {"name": "amount", "type": "double"},
                        ]
                    }
                },
            }
        ],
        outputs=[
            {
                "namespace": "warehouse",
                "name": "curated_orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "integer"},
                            {"name": "customer_id", "type": "integer"},
                            {"name": "amount_usd", "type": "double"},
                        ]
                    },
                    "columnLineage": {
                        "fields": {
                            "amount_usd": {
                                "inputFields": [
                                    {"namespace": "warehouse", "name": "raw_orders", "field": "amount"}
                                ]
                            },
                            "order_id": {
                                "inputFields": [
                                    {"namespace": "warehouse", "name": "raw_orders", "field": "order_id"}
                                ]
                            },
                            "customer_id": {
                                "inputFields": [
                                    {"namespace": "warehouse", "name": "raw_orders", "field": "customer_id"}
                                ]
                            },
                        }
                    },
                },
            }
        ],
    )
    result = engine.openlineage.ingest(complete, idempotency_key=f"{run_id}:COMPLETE")

    legacy = LineageEvent(
        pipeline_run_id=run_id,
        sequence=1,
        transform=TransformRecord(
            input_dataset="warehouse.raw_orders",
            output_dataset="warehouse.curated_orders",
            input_schema=SchemaVersion(version=1, fields={"order_id": "int", "amount": "float"}),
            output_schema=SchemaVersion(version=2, fields={"order_id": "int", "amount_usd": "float"}),
            row_count=1000,
        ),
    )
    engine.ingest_sync(legacy, idempotency_key=f"legacy:{run_id}")

    impact = engine.impact.field_impact("warehouse.raw_orders", "amount")
    return {
        "run_id": run_id,
        "openlineage": result,
        "impact": impact,
        "integrity": engine.verify(),
        "acknowledged_writes": engine.storage.count_acknowledged(),
    }
