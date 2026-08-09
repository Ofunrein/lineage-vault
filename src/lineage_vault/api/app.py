from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

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
from ..storage.config import StorageConfig, load_storage_config


class IngestBody(BaseModel):
    pipeline_run_id: str
    sequence: int
    input_dataset: str
    output_dataset: str
    input_schema: dict[str, str]
    output_schema: dict[str, str]
    row_count: int = 0
    idempotency_key: str | None = None


class OpenLineageBatchBody(BaseModel):
    events: list[OpenLineageRunEvent] = Field(default_factory=list)


def _error(code: str, message: str, status: int = 400) -> HTTPException:
    return HTTPException(status_code=status, detail={"error": code, "message": message})


def create_app(
    data_dir: str = ".data",
    *,
    config: StorageConfig | None = None,
) -> FastAPI:
    configure_logging()
    storage_config = config or load_storage_config(data_dir=data_dir)
    engine = LineageVaultEngine(data_dir, config=storage_config)
    max_batch = storage_config.max_batch_size
    max_bytes = storage_config.max_request_bytes

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        engine.recover()
        yield
        engine.close()

    app = FastAPI(title="LineageVault", version="0.3.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"error": "validation_error", "message": "invalid request body", "details": exc.errors()},
        )

    @app.middleware("http")
    async def limit_request_size(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > max_bytes:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "error": "payload_too_large",
                            "message": f"request exceeds {max_bytes} bytes",
                        },
                    )
            except ValueError:
                pass
        return await call_next(request)

    @app.get("/health")
    def health() -> dict[str, Any]:
        ok = engine.verify()
        INTEGRITY_OK.set(1 if ok else 0)
        ACKNOWLEDGED_WRITES.set(engine.storage.count_acknowledged())
        return {
            "status": "ok" if ok else "degraded",
            "integrity": ok,
            "acknowledged_writes": engine.storage.count_acknowledged(),
            "backend": engine.config.backend,
        }

    @app.get("/ready")
    def ready() -> dict[str, Any]:
        try:
            count = engine.storage.count_acknowledged()
            ok = engine.verify()
        except Exception as exc:
            raise _error("not_ready", str(exc), 503) from exc
        if not ok:
            raise _error("integrity_failed", "ledger integrity check failed", 503)
        return {"ready": True, "acknowledged_writes": count, "backend": engine.config.backend}

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

    @app.post("/openlineage/batch")
    def post_openlineage_batch(body: OpenLineageBatchBody, request: Request) -> dict[str, Any]:
        if len(body.events) > max_batch:
            raise _error(
                "batch_too_large",
                f"batch size {len(body.events)} exceeds limit {max_batch}",
                413,
            )
        if not body.events:
            return {"results": [], "count": 0, "integrity": engine.verify()}
        header_key = request.headers.get("Idempotency-Key")
        keys = [header_key] * len(body.events) if header_key else None
        with INGEST_LATENCY.time():
            results = engine.openlineage.ingest_batch(body.events, idempotency_keys=keys)
            for result in results:
                if result["duplicate"]:
                    EVENTS_DUPLICATE.inc()
                else:
                    EVENTS_INGESTED.inc()
            return {
                "results": results,
                "count": len(results),
                "integrity": engine.verify(),
            }

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
            if at.endswith("Z"):
                ts = datetime.fromisoformat(at[:-1]).replace(tzinfo=datetime.UTC)
            else:
                ts = datetime.fromisoformat(at)
        except ValueError as e:
            raise _error("invalid_timestamp", str(e), 400) from e
        return engine.timetravel.snapshot(dataset_id, ts)

    @app.post("/verify")
    def verify() -> dict[str, bool]:
        return {"valid": engine.verify()}

    @app.post("/demo/run")
    def demo_run() -> dict[str, Any]:
        return run_demo_pipeline(engine)

    return app
