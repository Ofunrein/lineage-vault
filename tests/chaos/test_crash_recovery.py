"""Gauntlet L1 — ledger+wal crash recovery."""
import tempfile
from pathlib import Path

from lineage_vault.ledger.store import LedgerStore
from lineage_vault.wal.recovery import WalBuffer


def test_gauntlet_l1_no_lost_records():
    with tempfile.TemporaryDirectory() as d:
        ledger = LedgerStore(Path(d) / "l.db")
        wal = WalBuffer(Path(d) / "w.db")
        for i in range(100):
            eid = f"ev-{i}"
            wal.stage(eid, {"seq": i})
            if i % 3 == 0:
                wal.commit(eid, ledger.append)
        replayed = wal.replay_pending(ledger.append)
        assert replayed > 0
        assert ledger.verify_integrity()
        assert len(ledger.all_entries()) == 100
        ledger.close()
        wal.close()
