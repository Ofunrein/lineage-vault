from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import networkx as nx
import psycopg
from psycopg.rows import tuple_row
from psycopg_pool import ConnectionPool

from .interface import AcknowledgedWrite, BatchWriteItem, LedgerEntry, StorageBackend
from .ledger_chain import GENESIS_HASH, entry_hash, payload_json
from .postgres_migrations import POSTGRES_MIGRATIONS

LEDGER_ADVISORY_LOCK_KEY = 81_508_150


class PostgresStorageBackend(StorageBackend):
    """PostgreSQL backend with bounded connection pool and transactional ledger writes."""

    def __init__(self, dsn: str, *, pool_min: int = 1, pool_max: int = 10) -> None:
        self._dsn = dsn
        self._pool = ConnectionPool(
            dsn,
            min_size=pool_min,
            max_size=pool_max,
            kwargs={"autocommit": False, "row_factory": tuple_row},
        )
        self._graph_lock = threading.Lock()
        self._graph = nx.DiGraph()
        self._migrate()
        self._load_graph_cache()

    @contextmanager
    def _borrow(self) -> Iterator[psycopg.Connection]:
        with self._pool.connection() as conn:
            yield conn

    def _migrate(self) -> int:
        with self._borrow() as conn:
            try:
                conn.execute(
                    """CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    )"""
                )
                row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
                current = row[0] or 0
                applied = 0
                now = datetime.now(UTC).isoformat()
                for version, ddl in POSTGRES_MIGRATIONS:
                    if version <= current:
                        continue
                    conn.execute(ddl)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (%s, %s)",
                        (version, now),
                    )
                    applied += 1
                conn.commit()
                return applied
            except Exception:
                conn.rollback()
                raise

    def migrate(self) -> int:
        return self._migrate()

    def _load_graph_cache(self) -> None:
        with self._borrow() as conn:
            rows = conn.execute("SELECT src, dst FROM dataset_edges").fetchall()
        with self._graph_lock:
            self._graph.clear()
            for src, dst in rows:
                self._graph.add_edge(src, dst)

    def _last_hash(self, conn: psycopg.Connection) -> str:
        row = conn.execute(
            "SELECT entry_hash FROM ledger ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else GENESIS_HASH

    def close(self) -> None:
        self._pool.close()

    def stage_partial(self, event_id: str, payload: dict[str, Any]) -> None:
        payload_text = payload_json(payload)
        with self._borrow() as conn:
            try:
                conn.execute(
                    """INSERT INTO wal_staging(event_id, payload, committed)
                       VALUES (%s, %s, 0)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (event_id, payload_text),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _append_ledger(
        self, conn: psycopg.Connection, event_id: str, payload: dict[str, Any]
    ) -> LedgerEntry:
        payload_text = payload_json(payload)
        prev = self._last_hash(conn)
        digest = entry_hash(prev, event_id, payload_text)
        row = conn.execute(
            """INSERT INTO ledger(event_id, payload, prev_hash, entry_hash)
               VALUES (%s, %s, %s, %s)
               RETURNING seq""",
            (event_id, payload_text, prev, digest),
        ).fetchone()
        return LedgerEntry(
            seq=row[0],
            event_id=event_id,
            payload=payload,
            prev_hash=prev,
            entry_hash=digest,
        )

    def acknowledge_write(
        self,
        *,
        idempotency_key: str,
        event_id: str,
        payload: dict[str, Any],
    ) -> AcknowledgedWrite:
        return self.acknowledge_writes_batch(
            [BatchWriteItem(idempotency_key=idempotency_key, event_id=event_id, payload=payload)]
        )[0]

    def acknowledge_writes_batch(self, items: list[BatchWriteItem]) -> list[AcknowledgedWrite]:
        if not items:
            return []
        with self._borrow() as conn:
            try:
                conn.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (LEDGER_ADVISORY_LOCK_KEY,),
                )
                keys = [item.idempotency_key for item in items]
                existing_rows = conn.execute(
                    "SELECT idempotency_key, event_id FROM idempotency WHERE idempotency_key = ANY(%s)",
                    (keys,),
                ).fetchall()
                existing = {row[0]: row[1] for row in existing_rows}
                results: list[AcknowledgedWrite] = []
                now = datetime.now(UTC).isoformat()
                for item in items:
                    if item.idempotency_key in existing:
                        results.append(
                            AcknowledgedWrite(
                                event_id=existing[item.idempotency_key],
                                idempotency_key=item.idempotency_key,
                                acknowledged=True,
                                duplicate=True,
                            )
                        )
                        continue
                    payload_text = payload_json(item.payload)
                    conn.execute(
                        "INSERT INTO idempotency(idempotency_key, event_id, created_at) VALUES (%s, %s, %s)",
                        (item.idempotency_key, item.event_id, now),
                    )
                    conn.execute(
                        """INSERT INTO wal_staging(event_id, payload, committed)
                           VALUES (%s, %s, 0)
                           ON CONFLICT (event_id) DO NOTHING""",
                        (item.event_id, payload_text),
                    )
                    conn.execute(
                        "UPDATE wal_staging SET committed=1 WHERE event_id=%s",
                        (item.event_id,),
                    )
                    self._append_ledger(conn, item.event_id, item.payload)
                    existing[item.idempotency_key] = item.event_id
                    results.append(
                        AcknowledgedWrite(
                            event_id=item.event_id,
                            idempotency_key=item.idempotency_key,
                            acknowledged=True,
                            duplicate=False,
                        )
                    )
                conn.commit()
                return results
            except Exception:
                conn.rollback()
                raise

    def recover_uncommitted(self) -> int:
        with self._borrow() as conn:
            try:
                conn.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (LEDGER_ADVISORY_LOCK_KEY,),
                )
                rows = conn.execute(
                    "SELECT event_id, payload FROM wal_staging WHERE committed=0 ORDER BY id"
                ).fetchall()
                count = 0
                for event_id, payload_text in rows:
                    payload = json.loads(payload_text)
                    dup = conn.execute(
                        "SELECT 1 FROM ledger WHERE event_id=%s", (event_id,)
                    ).fetchone()
                    if dup:
                        conn.execute(
                            "UPDATE wal_staging SET committed=1 WHERE event_id=%s", (event_id,)
                        )
                        continue
                    self._append_ledger(conn, event_id, payload)
                    conn.execute(
                        "UPDATE wal_staging SET committed=1 WHERE event_id=%s", (event_id,)
                    )
                    count += 1
                conn.commit()
                return count
            except Exception:
                conn.rollback()
                raise

    def verify_integrity(self) -> bool:
        with self._borrow() as conn:
            rows = conn.execute(
                "SELECT seq, event_id, payload, prev_hash, entry_hash FROM ledger ORDER BY seq"
            ).fetchall()
        expected_prev = GENESIS_HASH
        for _, event_id, payload, prev_hash, digest in rows:
            if prev_hash != expected_prev:
                return False
            if entry_hash(prev_hash, event_id, payload) != digest:
                return False
            expected_prev = digest
        return True

    def count_acknowledged(self) -> int:
        with self._borrow() as conn:
            row = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()
        return int(row[0]) if row else 0

    def all_ledger_entries(self) -> list[LedgerEntry]:
        with self._borrow() as conn:
            rows = conn.execute(
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
        with self._borrow() as conn:
            conn.execute(
                """INSERT INTO dataset_edges(src,dst,transform_id,event_time,schema_version,payload)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (src, dst, transform_id, event_time.isoformat(), schema_version, json.dumps(payload)),
            )
            conn.commit()
        with self._graph_lock:
            self._graph.add_edge(src, dst)

    def upstream_datasets(self, dataset_id: str) -> list[str]:
        with self._graph_lock:
            if dataset_id not in self._graph:
                return []
            return sorted(nx.ancestors(self._graph, dataset_id))

    def downstream_datasets(self, dataset_id: str) -> list[str]:
        with self._graph_lock:
            if dataset_id not in self._graph:
                return []
            return sorted(nx.descendants(self._graph, dataset_id))

    def edges_at(self, at: datetime) -> list[dict[str, Any]]:
        with self._borrow() as conn:
            rows = conn.execute(
                "SELECT src,dst,transform_id,event_time,schema_version,payload FROM dataset_edges"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for src, dst, tid, ts, sv, pl in rows:
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
        with self._borrow() as conn:
            for out_field, in_field in field_map.items():
                conn.execute(
                    """INSERT INTO field_mappings(run_id, output_dataset, input_dataset, output_field, input_field)
                       VALUES (%s,%s,%s,%s,%s)""",
                    (run_id, output_dataset, input_dataset, out_field, in_field),
                )
            conn.commit()

    def field_impact(self, dataset: str, field: str) -> dict[str, Any]:
        with self._borrow() as conn:
            impacted_fields: list[dict[str, str]] = []
            frontier = [(dataset, field)]
            seen: set[tuple[str, str]] = set()
            while frontier:
                ds, fld = frontier.pop(0)
                if (ds, fld) in seen:
                    continue
                seen.add((ds, fld))
                rows = conn.execute(
                    """SELECT output_dataset, output_field FROM field_mappings
                       WHERE input_dataset=%s AND input_field=%s""",
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
        with self._borrow() as conn:
            conn.execute(
                """INSERT INTO run_events(run_id, event_type, payload) VALUES (%s,%s,%s)
                   ON CONFLICT(run_id, event_type) DO UPDATE SET payload=EXCLUDED.payload""",
                (run_id, event_type, payload_json(payload)),
            )
            conn.commit()

    def get_run_event(self, run_id: str, event_type: str) -> dict[str, Any] | None:
        with self._borrow() as conn:
            row = conn.execute(
                "SELECT payload FROM run_events WHERE run_id=%s AND event_type=%s",
                (run_id, event_type),
            ).fetchone()
        return json.loads(row[0]) if row else None

    @property
    def pool_size(self) -> int:
        return self._pool.max_size
