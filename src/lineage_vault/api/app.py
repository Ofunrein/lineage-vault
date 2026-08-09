from __future__ import annotations

import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

from ..demo.pipeline import run_demo_pipeline
from ..engine import LineageVaultEngine
from ..models.events import LineageEvent, SchemaVersion, TransformRecord
from ..observability.metrics import (
    ACKNOWLEDGED_WRITES,
    EVENTS_DUPLICATE,
    EVENTS_INGESTED,
    INGEST_LATENCY,
    INTEGRITY_OK,
    configure_logging,
    metrics_payload,
)
from ..openlineage.models import OpenLineageRunEvent


class IngestBody(BaseModel):
    pipeline_run_id: str
    sequence: int
    input_dataset: str
    output_dataset: str
    input_schema: dict[str, str]
    output_schema: dict[str, str]
    row_count: int = 0
    idempotency_key: str | None = None


def create_app(data_dir: str = ".data") -> FastAPI:
    configure_logging()
    engine = LineageVaultEngine(data_dir)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        engine.recover()
        yield
        engine.close()

    app = FastAPI(title="LineageVault", version="0.2.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, Any]:
        ok = engine.verify()
        INTEGRITY_OK.set(1 if ok else 0)
        ACKNOWLEDGED_WRITES.set(engine.storage.count_acknowledged())
        return {
            "status": "ok" if ok else "degraded",
            "integrity": ok,
            "acknowledged_writes": engine.storage.count_acknowledged(),
        }

    @app.get("/metrics")
    def metrics() -> Response:
        body, ctype = metrics_payload()
        return Response(content=body, media_type=ctype)

    @app.post("/events")
    def post_event(body: IngestBody) -> dict[str, Any]:
        with INGEST_LATENCY.time():
            event = LineageEvent(
                pipeline_run_id=body.pipeline_run_id,
                sequence=body.sequence,
                transform=TransformRecord(
                    input_dataset=body.input_dataset,
                    output_dataset=body.output_dataset,
                    input_schema=SchemaVersion(version=1, fields=body.input_schema),
                    output_schema=SchemaVersion(version=2, fields=body.output_schema),
                    row_count=body.row_count,
                ),
            )
            engine.ingest_sync(event, idempotency_key=body.idempotency_key)
            EVENTS_INGESTED.inc()
            return {"event_id": event.event_id, "integrity": engine.verify()}

    @app.post("/openlineage")
    def post_openlineage(event: OpenLineageRunEvent, request: Request) -> dict[str, Any]:
        key = request.headers.get("Idempotency-Key")
        with INGEST_LATENCY.time():
            result = engine.openlineage.ingest(event, idempotency_key=key)
            if result["duplicate"]:
                EVENTS_DUPLICATE.inc()
            else:
                EVENTS_INGESTED.inc()
            return result

    @app.get("/lineage/{dataset_id}")
    def lineage(dataset_id: str) -> dict[str, Any]:
        return {
            "dataset_id": dataset_id,
            "upstream": engine.graph.upstream(dataset_id),
            "downstream": engine.graph.downstream(dataset_id),
        }

    @app.get("/impact/dataset/{dataset_id}")
    def dataset_impact(dataset_id: str) -> dict[str, Any]:
        return engine.impact.dataset_impact(dataset_id)

    @app.get("/impact/field/{dataset_id}/{field}")
    def field_impact(dataset_id: str, field: str) -> dict[str, Any]:
        return engine.impact.field_impact(dataset_id, field)

    @app.get("/snapshot")
    def snapshot(dataset_id: str, at: str) -> dict[str, Any]:
        try:
            ts = datetime.fromisoformat(at.replace("Z", "+00:00"))
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        return engine.timetravel.snapshot(dataset_id, ts)

    @app.post("/verify")
    def verify() -> dict[str, bool]:
        return {"valid": engine.verify()}

    @app.post("/demo/run")
    def demo_run() -> dict[str, Any]:
        return run_demo_pipeline(engine)

    return app
