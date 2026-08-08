from __future__ import annotations
from pathlib import Path
from .models.events import LineageEvent
from .compliance.agent import ComplianceAgent
from .graph.store import GraphStore
from .ledger.store import LedgerStore
from .schema.engine import SchemaEngine
from .timetravel.query import TimeTravelEngine
from .wal.recovery import WalBuffer

class LineageVaultEngine:
    def __init__(self, data_dir: str | Path = ".data") -> None:
        d = Path(data_dir)
        d.mkdir(parents=True, exist_ok=True)
        self.ledger = LedgerStore(d / "ledger.db")
        self.wal = WalBuffer(d / "wal.db")
        self.graph = GraphStore(d / "graph.db")
        self.timetravel = TimeTravelEngine(self.graph, self.ledger)
        self.schema = SchemaEngine()
        self.compliance = ComplianceAgent()

    def _persist_event(self, event: LineageEvent) -> None:
        payload = event.model_dump(mode="json")
        payload["output_dataset"] = event.transform.output_dataset
        payload["input_dataset"] = event.transform.input_dataset
        payload["timestamp"] = event.timestamp.isoformat()
        self.wal.stage(event.event_id, payload)
        self.wal.commit(event.event_id, self.ledger.append)
        status, compat = self.compliance.check(event)
        event.transform.compliance = status
        event.transform.compat = compat
        self.graph.add_edge(
            event.transform.input_dataset, event.transform.output_dataset,
            event.transform.transform_id, event.timestamp,
            event.transform.output_schema.version, payload,
        )

    def ingest_sync(self, event: LineageEvent) -> None:
        self._persist_event(event)

    def recover(self) -> int:
        return self.wal.replay_pending(self.ledger.append)

    def verify(self) -> bool:
        return self.ledger.verify_integrity()

    def close(self) -> None:
        self.ledger.close()
        self.wal.close()
        self.graph.close()
