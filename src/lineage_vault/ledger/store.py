from __future__ import annotations
import hashlib, json, sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

GENESIS_HASH = "0" * 64

@dataclass(frozen=True)
class LedgerEntry:
    seq: int
    event_id: str
    payload: dict[str, Any]
    prev_hash: str
    entry_hash: str

class LedgerStore:
    def __init__(self, db_path: str | Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                prev_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL
            )
        """)
        self._conn.commit()

    def _last_hash(self) -> str:
        row = self._conn.execute("SELECT entry_hash FROM ledger ORDER BY seq DESC LIMIT 1").fetchone()
        return row[0] if row else GENESIS_HASH

    @staticmethod
    def _hash(prev: str, event_id: str, payload_json: str) -> str:
        return hashlib.sha256(f"{prev}|{event_id}|{payload_json}".encode()).hexdigest()

    def append(self, event_id: str, payload: dict[str, Any]) -> LedgerEntry:
        payload_json = json.dumps(payload, sort_keys=True)
        prev = self._last_hash()
        entry_hash = self._hash(prev, event_id, payload_json)
        cur = self._conn.execute(
            "INSERT INTO ledger (event_id, payload, prev_hash, entry_hash) VALUES (?,?,?,?)",
            (event_id, payload_json, prev, entry_hash),
        )
        self._conn.commit()
        return LedgerEntry(seq=cur.lastrowid or 0, event_id=event_id, payload=payload,
                           prev_hash=prev, entry_hash=entry_hash)

    def verify_integrity(self) -> bool:
        expected_prev = GENESIS_HASH
        for _, event_id, payload, prev_hash, entry_hash in self._conn.execute(
            "SELECT seq, event_id, payload, prev_hash, entry_hash FROM ledger ORDER BY seq"
        ):
            if prev_hash != expected_prev:
                return False
            if self._hash(prev_hash, event_id, payload) != entry_hash:
                return False
            expected_prev = entry_hash
        return True

    def all_entries(self) -> list[LedgerEntry]:
        rows = self._conn.execute(
            "SELECT seq, event_id, payload, prev_hash, entry_hash FROM ledger ORDER BY seq"
        ).fetchall()
        return [LedgerEntry(r[0], r[1], json.loads(r[2]), r[3], r[4]) for r in rows]

    def close(self) -> None:
        self._conn.close()
