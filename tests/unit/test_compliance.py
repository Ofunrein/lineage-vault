from lineage_vault.compliance.agent import ComplianceAgent
from lineage_vault.models.events import (
    ComplianceStatus,
    LineageEvent,
    SchemaVersion,
    TransformRecord,
)


def test_quarantine_ssn():
    agent = ComplianceAgent()
    ev = LineageEvent(
        pipeline_run_id="r", sequence=1,
        transform=TransformRecord(
            input_dataset="a", output_dataset="b",
            input_schema=SchemaVersion(version=1, fields={"id": "int"}),
            output_schema=SchemaVersion(version=2, fields={"ssn": "str"}),
        ),
    )
    status, _ = agent.check(ev)
    assert status == ComplianceStatus.QUARANTINED
