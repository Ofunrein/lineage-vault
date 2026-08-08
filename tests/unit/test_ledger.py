import json, tempfile
from pathlib import Path
from lineage_vault.ledger.store import LedgerStore, GENESIS_HASH

def test_tamper_detection():
    with tempfile.TemporaryDirectory() as d:
        s = LedgerStore(Path(d)/"l.db")
        s.append("e1", {"a": 1})
        s.append("e2", {"b": 2})
        assert s.verify_integrity()
        s._conn.execute("UPDATE ledger SET payload=?", (json.dumps({"hack": True}),))
        s._conn.commit()
        assert not s.verify_integrity()
        s.close()

def test_genesis():
    with tempfile.TemporaryDirectory() as d:
        s = LedgerStore(Path(d)/"l.db")
        e = s.append("x", {"v": 1})
        assert e.prev_hash == GENESIS_HASH
        s.close()
