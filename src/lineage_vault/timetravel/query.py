from __future__ import annotations

from datetime import datetime
from typing import Any

from ..graph.store import GraphStore
from ..ledger.store import LedgerStore


class TimeTravelEngine:
    def __init__(self, graph: GraphStore, ledger: LedgerStore) -> None:
        self._graph = graph
        self._ledger = ledger

    def snapshot(self, dataset_id: str, at: datetime) -> dict[str, Any]:
        edges = [e for e in self._graph.edges_at(at)
                 if e["dst"] == dataset_id or e["src"] == dataset_id]
        events = [
            e.payload for e in self._ledger.all_entries()
            if datetime.fromisoformat(e.payload.get("timestamp", "1970-01-01")) <= at
            and e.payload.get("output_dataset") in (dataset_id, None)
        ]
        return {"dataset_id": dataset_id, "at": at.isoformat(), "edges": edges, "events": events}

    def replay_at(self, dataset_id: str, at: datetime) -> dict[str, Any]:
        return self.snapshot(dataset_id, at)
