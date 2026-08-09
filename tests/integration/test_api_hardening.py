"""API hardening: readiness, batch limits, stable errors."""

from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from lineage_vault.api.app import create_app
from lineage_vault.storage.config import StorageConfig


def test_ready_endpoint():
    with tempfile.TemporaryDirectory() as d:
        client = TestClient(create_app(d))
        r = client.get("/ready")
        assert r.status_code == 200
        assert r.json()["ready"] is True


def test_batch_endpoint_rejects_oversized_batch():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        cfg = StorageConfig(
            backend="sqlite",
            data_dir=root,
            sqlite_path=root / "vault.db",
            max_batch_size=2,
            max_request_bytes=1_048_576,
        )
        client = TestClient(create_app(d, config=cfg))
        events = [
            {
                "eventType": "COMPLETE",
                "eventTime": "2026-01-01T00:00:00Z",
                "run": {"runId": f"r{i}"},
                "job": {"namespace": "t", "name": "j"},
            }
            for i in range(3)
        ]
        r = client.post("/openlineage/batch", json={"events": events})
        assert r.status_code == 413
        body = r.json()
        assert body["detail"]["error"] == "batch_too_large"


def test_validation_error_shape():
    with tempfile.TemporaryDirectory() as d:
        client = TestClient(create_app(d))
        r = client.post("/openlineage", json={"bad": True})
        assert r.status_code == 422
        body = r.json()
        assert body["error"] == "validation_error"


def test_batch_ingest_success():
    with tempfile.TemporaryDirectory() as d:
        client = TestClient(create_app(d))
        events = [
            {
                "eventType": "COMPLETE",
                "eventTime": "2026-01-01T00:00:00Z",
                "run": {"runId": "batch-1"},
                "job": {"namespace": "t", "name": "j"},
                "inputs": [{"namespace": "t", "name": "in"}],
                "outputs": [{"namespace": "t", "name": "out"}],
            }
        ]
        r = client.post("/openlineage/batch", json={"events": events})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["integrity"] is True
