from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import networkx as nx


class GraphStore:
    def __init__(self, db_path: str | Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                src TEXT NOT NULL, dst TEXT NOT NULL, transform_id TEXT NOT NULL,
                timestamp TEXT NOT NULL, schema_version INTEGER NOT NULL, payload TEXT NOT NULL
            )
        """)
        self._conn.commit()
        self._g = nx.DiGraph()

    def add_edge(self, src: str, dst: str, transform_id: str,
                 ts: datetime, schema_version: int, payload: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO edges (src,dst,transform_id,timestamp,schema_version,payload) VALUES (?,?,?,?,?,?)",
            (src, dst, transform_id, ts.isoformat(), schema_version, json.dumps(payload)),
        )
        self._conn.commit()
        self._g.add_edge(src, dst)

    def upstream(self, dataset_id: str) -> list[str]:
        return sorted(nx.ancestors(self._g, dataset_id)) if dataset_id in self._g else []

    def edges_at(self, at: datetime) -> list[dict[str, Any]]:
        out = []
        for src, dst, tid, ts, sv, pl in self._conn.execute(
            "SELECT src,dst,transform_id,timestamp,schema_version,payload FROM edges"
        ):
            if datetime.fromisoformat(ts) <= at:
                out.append({"src": src, "dst": dst, "transform_id": tid,
                            "timestamp": ts, "schema_version": sv, "payload": json.loads(pl)})
        return out

    def close(self) -> None:
        self._conn.close()
