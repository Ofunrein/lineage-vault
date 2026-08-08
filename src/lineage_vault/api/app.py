from __future__ import annotations
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from ..engine import LineageVaultEngine
from ..models.events import LineageEvent, SchemaVersion, TransformRecord

class IngestBody(BaseModel):
    pipeline_run_id: str
    sequence: int
    input_dataset: str
    output_dataset: str
    input_schema: dict[str, str]
    output_schema: dict[str, str]
    row_count: int = 0

def create_app(data_dir: str = ".data") -> FastAPI:
    app = FastAPI(title="LineageVault", version="0.1.0")
    engine = LineageVaultEngine(data_dir)

    @app.on_event("startup")
    def _recover() -> None:
        engine.recover()

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "integrity": engine.verify()}

    @app.post("/events")
    def post_event(body: IngestBody) -> dict:
        event = LineageEvent(
            pipeline_run_id=body.pipeline_run_id, sequence=body.sequence,
            transform=TransformRecord(
                input_dataset=body.input_dataset, output_dataset=body.output_dataset,
                input_schema=SchemaVersion(version=1, fields=body.input_schema),
                output_schema=SchemaVersion(version=2, fields=body.output_schema),
                row_count=body.row_count,
            ),
        )
        engine.ingest_sync(event)
        return {"event_id": event.event_id, "integrity": engine.verify()}

    @app.get("/lineage/{dataset_id}")
    def lineage(dataset_id: str) -> dict:
        return {"dataset_id": dataset_id, "upstream": engine.graph.upstream(dataset_id)}

    @app.get("/snapshot")
    def snapshot(dataset_id: str, at: str) -> dict:
        try:
            ts = datetime.fromisoformat(at.replace("Z", "+00:00"))
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        return engine.timetravel.snapshot(dataset_id, ts)

    @app.post("/verify")
    def verify() -> dict:
        return {"valid": engine.verify()}

    return app
