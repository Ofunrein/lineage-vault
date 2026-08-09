from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

EVENTS_INGESTED = Counter("lineage_vault_events_ingested_total", "Total lineage events ingested")
EVENTS_DUPLICATE = Counter("lineage_vault_events_duplicate_total", "Duplicate idempotent events")
INGEST_LATENCY = Histogram("lineage_vault_ingest_latency_seconds", "Event ingest latency")
ACKNOWLEDGED_WRITES = Gauge("lineage_vault_acknowledged_writes", "Acknowledged write count")
INTEGRITY_OK = Gauge("lineage_vault_integrity_ok", "Ledger integrity (1=ok, 0=fail)")


def configure_logging(level: int = logging.INFO) -> None:
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "ts": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            if record.exc_info:
                payload["exc"] = self.formatException(record.exc_info)
            return json.dumps(payload, ensure_ascii=True)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("lineage_vault")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
