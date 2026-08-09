from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx

from .interface import AcknowledgedWrite, LedgerEntry, StorageBackend
from .migrations import MIGRATIONS

GENESIS_HASH = "0" * 64


class SQLiteStorageBackend(StorageBackend):
    """Transactional SQLite backend with WAL mode, idempotency, and crash recovery."""

    def __init__(self, db_path: str | Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._graph = nx.DiGraph()
        self._migrate()
        self._load_graph_cache()

    def _migrate(self) -> int:
        with self._lock:
            try:
                self._conn.execute(
                    """CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    )"""
                )
                row = self._conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
                current = row[0] or 0
                applied = 0
                now = datetime.now(timezone.utc).isoformat()
                for version, sql in MIGRATIONS:
                    if version <= current:
                        continue
                    self._conn.executescript(sql)
                    self._conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (version, now),
                    )
                    applied += 1
                self._conn.commit()
                return applied
            except Exception:
                self._conn.rollback()
                raise

    def migrate(self) -> int:
        return self._migrate()

    def _load_graph_cache(self) -> None:
        self._graph.clear()
        rows = self._conn.execute("SELECT src, dst FROM dataset_edges").fetchall()
        for src, dst in rows:
            self._graph.add_edge(src, dst)

    @staticmethod
    def _hash(prev: str, event_id: str, payload_json: str) -> str:
        return hashlib.sha256(f"{prev}|{event_id}|{payload_json}".encode()).hexdigest()

    def _last_hash(self) -> str:
        row = self._conn.execute(
            "SELECT entry_hash FROM ledger ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else GENESIS_HASH

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def stage_partial(self, event_id: str, payload: dict[str, Any]) -> None:
        payload_json = json.dumps(payload, sort_keys=True)
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO wal_staging(event_id, payload, committed) VALUES (?,?,0)",
                    (event_id, payload_json),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _append_ledger(self, event_id: str, payload: dict[str, Any]) -> LedgerEntry:
        payload_json = json.dumps(payload, sort_keys=True)
        prev = self._last_hash()
        entry_hash = self._hash(prev, event_id, payload_json)
        cur = self._conn.execute(
            "INSERT INTO ledger(event_id, payload, prev_hash, entry_hash) VALUES (?,?,?,?)",
            (event_id, payload_json, prev, entry_hash),
        )
        return LedgerEntry(
            seq=cur.lastrowid or 0,
            event_id=event_id,
            payload=payload,
            prev_hash=prev,
            entry_hash=entry_hash,
        )

    def acknowledge_write(
        self,
        *,
        idempotency_key: str,
        event_id: str,
        payload: dict[str, Any],
    ) -> AcknowledgedWrite:
        payload_json = json.dumps(payload, sort_keys=True)
        with self._lock:
            try:
                existing = self._conn.execute(
                    "SELECT event_id FROM idempotency WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    self._conn.commit()
                    return AcknowledgedWrite(
                        event_id=existing[0],
                        idempotency_key=idempotency_key,
                        acknowledged=True,
                        duplicate=True,
                    )

                self._conn.execute(
                    "INSERT INTO idempotency(idempotency_key, event_id, created_at) VALUES (?,?,?)",
                    (idempotency_key, event_id, datetime.now(timezone.utc).isoformat()),
                )
                self._conn.execute(
                    "INSERT OR IGNORE INTO wal_staging(event_id, payload, committed) VALUES (?,?,0)",
                    (event_id, payload_json),
                )
                self._conn.execute(
                    "UPDATE wal_staging SET committed=1 WHERE event_id=?",
                    (event_id,),
                )
                self._append_ledger(event_id, payload)
                self._conn.commit()
                return AcknowledgedWrite(
                    event_id=event_id,
                    idempotency_key=idempotency_key,
                    acknowledged=True,
                    duplicate=False,
                )
            except Exception:
                self._conn.rollback()
                raise

    def recover_uncommitted(self) -> int:
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT event_id, payload FROM wal_staging WHERE committed=0 ORDER BY id"
                ).fetchall()
                count = 0
                for event_id, payload_json in rows:
                    payload = json.loads(payload_json)
                    dup = self._conn.execute(
                        "SELECT 1 FROM ledger WHERE event_id=?", (event_id,)
                    ).fetchone()
                    if dup:
                        self._conn.execute(
                            "UPDATE wal_staging SET committed=1 WHERE event_id=?", (event_id,)
                        )
                        continue
                    self._append_ledger(event_id, payload)
                    self._conn.execute(
                        "UPDATE wal_staging SET committed=1 WHERE event_id=?", (event_id,)
                    )
                    count += 1
                self._conn.commit()
                return count
            except Exception:
                self._conn.rollback()
                raise

    def verify_integrity(self) -> bool:
        rows = self._conn.execute(
            "SELECT seq, event_id, payload, prev_hash, entry_hash FROM ledger ORDER BY seq"
        ).fetchall()
        expected_prev = GENESIS_HASH
        for _, event_id, payload, prev_hash, entry_hash in rows:
            if prev_hash != expected_prev:
                return False
            if self._hash(prev_hash, event_id, payload) != entry_hash:
                return False
            expected_prev = entry_hash
        return True

    def count_acknowledged(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM ledger").fetchone()
        return int(row[0]) if row else 0

    def all_ledger_entries(self) -> list[LedgerEntry]:
        rows = self._conn.execute(
            "SELECT seq, event_id, payload, prev_hash, entry_hash FROM ledger ORDER BY seq"
        ).fetchall()
        return [
            LedgerEntry(r[0], r[1], json.loads(r[2]), r[3], r[4]) for r in rows
        ]

    def add_dataset_edge(
        self,
        *,
        src: str,
        dst: str,
        transform_id: str,
        event_time: datetime,
        schema_version: int,
        payload: dict[str, Any],
    ) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO dataset_edges(src,dst,transform_id,event_time,schema_version,payload)
                   VALUES (?,?,?,?,?,?)""",
                (src, dst, transform_id, event_time.isoformat(), schema_version, json.dumps(payload)),
            )
            self._conn.commit()
            self._graph.add_edge(src, dst)

    def upstream_datasets(self, dataset_id: str) -> list[str]:
        if dataset_id not in self._graph:
            return []
        return sorted(nx.ancestors(self._graph, dataset_id))

    def downstream_datasets(self, dataset_id: str) -> list[str]:
        if dataset_id not in self._graph:
            return []
        return sorted(nx.descendants(self._graph, dataset_id))

    def edges_at(self, at: datetime) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for src, dst, tid, ts, sv, pl in self._conn.execute(
            "SELECT src,dst,transform_id,event_time,schema_version,payload FROM dataset_edges"
        ):
            if datetime.fromisoformat(ts) <= at:
                out.append(
                    {
                        "src": src,
                        "dst": dst,
                        "transform_id": tid,
                        "event_time": ts,
                        "schema_version": sv,
                        "payload": json.loads(pl),
                    }
                )
        return out

    def record_field_mapping(
        self,
        *,
        run_id: str,
        output_dataset: str,
        input_dataset: str,
        field_map: dict[str, str],
    ) -> None:
        with self._lock:
            for out_field, in_field in field_map.items():
                self._conn.execute(
                    """INSERT INTO field_mappings(run_id, output_dataset, input_dataset, output_field, input_field)
                       VALUES (?,?,?,?,?)""",
                    (run_id, output_dataset, input_dataset, out_field, in_field),
                )
            self._conn.commit()

    def field_impact(self, dataset: str, field: str) -> dict[str, Any]:
        impacted_fields: list[dict[str, str]] = []
        frontier = [(dataset, field)]
        seen: set[tuple[str, str]] = set()
        while frontier:
            ds, fld = frontier.pop(0)
            if (ds, fld) in seen:
                continue
            seen.add((ds, fld))
            rows = self._conn.execute(
                """SELECT output_dataset, output_field FROM field_mappings
                   WHERE input_dataset=? AND input_field=?""",
                (ds, fld),
            ).fetchall()
            for out_ds, out_fld in rows:
                impacted_fields.append({"dataset": out_ds, "field": out_fld})
                frontier.append((out_ds, out_fld))
        downstream = self.downstream_datasets(dataset)
        return {
            "dataset": dataset,
            "field": field,
            "impacted_fields": impacted_fields,
            "downstream_datasets": downstream,
        }

    def store_run_event(self, run_id: str, event_type: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO run_events(run_id, event_type, payload) VALUES (?,?,?)
                   ON CONFLICT(run_id, event_type) DO UPDATE SET payload=excluded.payload""",
                (run_id, event_type, json.dumps(payload, sort_keys=True)),
            )
            self._conn.commit()

    def get_run_event(self, run_id: str, event_type: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT payload FROM run_events WHERE run_id=? AND event_type=?",
            (run_id, event_type),
        ).fetchone()
        return json.loads(row[0]) if row else None
