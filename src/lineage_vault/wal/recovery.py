from __future__ import annotations
import json, sqlite3
from pathlib import Path
from typing import Any, Callable

class WalBuffer:
    def __init__(self, wal_path: str | Path) -> None:
        self._path = Path(wal_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS wal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                committed INTEGER NOT NULL DEFAULT 0
            )
        """)
        self._conn.commit()

    def stage(self, event_id: str, payload: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO wal (event_id, payload, committed) VALUES (?,?,0)",
            (event_id, json.dumps(payload, sort_keys=True)),
        )
        self._conn.commit()

    def commit(self, event_id: str, ledger_append: Callable[[str, dict], Any]) -> None:
        row = self._conn.execute(
            "SELECT payload FROM wal WHERE event_id=? AND committed=0", (event_id,)
        ).fetchone()
        if not row:
            return
        ledger_append(event_id, json.loads(row[0]))
        self._conn.execute("UPDATE wal SET committed=1 WHERE event_id=?", (event_id,))
        self._conn.commit()

    def replay_pending(self, ledger_append: Callable[[str, dict], Any]) -> int:
        rows = self._conn.execute(
            "SELECT event_id, payload FROM wal WHERE committed=0 ORDER BY id"
        ).fetchall()
        for event_id, payload_json in rows:
            ledger_append(event_id, json.loads(payload_json))
            self._conn.execute("UPDATE wal SET committed=1 WHERE event_id=?", (event_id,))
        self._conn.commit()
        return len(rows)

    def close(self) -> None:
        self._conn.close()
