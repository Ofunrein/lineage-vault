from __future__ import annotations
from ..models.events import CompatLevel, ComplianceStatus, LineageEvent
from ..schema.engine import SchemaEngine

class ComplianceAgent:
    def __init__(self, forbidden_fields: set[str] | None = None) -> None:
        self._forbidden = forbidden_fields or {"ssn", "raw_pan", "password"}
        self._schema = SchemaEngine()

    def check(self, event: LineageEvent) -> tuple[ComplianceStatus, CompatLevel]:
        out_fields = set(event.transform.output_schema.fields)
        if self._forbidden & out_fields:
            return ComplianceStatus.QUARANTINED, event.transform.compat
        compat = self._schema.evaluate(
            event.transform.input_schema.fields,
            event.transform.output_schema.fields,
        ) if event.transform.input_schema.fields else CompatLevel.FORWARD
        if compat == CompatLevel.BREAKING:
            return ComplianceStatus.VIOLATION, compat
        return ComplianceStatus.OK, compat
