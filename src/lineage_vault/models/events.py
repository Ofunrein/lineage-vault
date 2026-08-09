from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class CompatLevel(str, Enum):
    FORWARD = "forward_compatible"
    BACKWARD = "backward_compatible"
    BREAKING = "breaking"

class ComplianceStatus(str, Enum):
    OK = "ok"
    VIOLATION = "violation"
    QUARANTINED = "quarantined"

class SchemaVersion(BaseModel):
    version: int
    fields: dict[str, str]

class TransformRecord(BaseModel):
    transform_id: str = Field(default_factory=lambda: str(uuid4()))
    input_dataset: str
    output_dataset: str
    input_schema: SchemaVersion
    output_schema: SchemaVersion
    row_count: int = 0
    compliance: ComplianceStatus = ComplianceStatus.OK
    compat: CompatLevel = CompatLevel.FORWARD

class LineageEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_run_id: str
    sequence: int
    transform: TransformRecord
    metadata: dict[str, Any] = Field(default_factory=dict)
