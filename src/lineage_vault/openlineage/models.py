from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class SchemaField(BaseModel):
    name: str
    type: str = "string"


class DatasetRef(BaseModel):
    namespace: str
    name: str
    fields: list[SchemaField] = Field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        return f"{self.namespace}.{self.name}"


class OpenLineageRunEvent(BaseModel):
    """OpenLineage-compatible RunEvent subset for ingestion."""

    eventType: Literal["START", "RUNNING", "COMPLETE", "FAIL", "ABORT"]
    eventTime: datetime
    run: dict[str, Any]
    job: dict[str, Any]
    inputs: list[dict[str, Any]] = Field(default_factory=list)
    outputs: list[dict[str, Any]] = Field(default_factory=list)
    producer: str = "https://github.com/Ofunrein/lineage-vault"

    @property
    def run_id(self) -> str:
        return str(self.run.get("runId", ""))

    @property
    def job_name(self) -> str:
        return f"{self.job.get('namespace', 'default')}.{self.job.get('name', 'job')}"

    def datasets_in(self) -> list[DatasetRef]:
        return [self._parse_dataset(d) for d in self.inputs]

    def datasets_out(self) -> list[DatasetRef]:
        return [self._parse_dataset(d) for d in self.outputs]

    @staticmethod
    def _parse_dataset(raw: dict[str, Any]) -> DatasetRef:
        ns = raw.get("namespace", "default")
        name = raw.get("name", "unknown")
        fields: list[SchemaField] = []
        facets = raw.get("facets", {}) or {}
        schema = facets.get("schema", {}) or {}
        for f in schema.get("fields", []):
            fields.append(SchemaField(name=f.get("name", ""), type=f.get("type", "string")))
        column_lineage = facets.get("columnLineage", {}) or {}
        for out_field, spec in column_lineage.get("fields", {}).items():
            fields.append(SchemaField(name=out_field, type=spec.get("type", "string")))
        return DatasetRef(namespace=ns, name=name, fields=fields)

    def field_mappings(self) -> list[tuple[str, str, str, dict[str, str]]]:
        """Return (output_dataset, input_dataset, run_id, field_map) tuples."""
        mappings: list[tuple[str, str, str, dict[str, str]]] = []
        for out_raw in self.outputs:
            out_ds = DatasetRef(
                namespace=out_raw.get("namespace", "default"),
                name=out_raw.get("name", "unknown"),
            )
            facets = out_raw.get("facets", {}) or {}
            column_lineage = facets.get("columnLineage", {}) or {}
            fields = column_lineage.get("fields", {})
            if not fields:
                continue
            for in_raw in self.inputs:
                in_ds = DatasetRef(
                    namespace=in_raw.get("namespace", "default"),
                    name=in_raw.get("name", "unknown"),
                )
                field_map: dict[str, str] = {}
                for out_field, spec in fields.items():
                    inputs = spec.get("inputFields", [])
                    if inputs:
                        field_map[out_field] = inputs[0].get("field", out_field)
                if field_map:
                    mappings.append((out_ds.qualified_name, in_ds.qualified_name, self.run_id, field_map))
        return mappings
