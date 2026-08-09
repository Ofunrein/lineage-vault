from lineage_vault.lifecycle.optimizer import LifecycleOptimizer


def test_cold_path():
    opt = LifecycleOptimizer()
    recs = opt.analyze([
        {"dataset_id": "hot", "access_count": 100, "bytes": 1e9, "tier": "hot"},
        {"dataset_id": "cold", "access_count": 2, "bytes": 1e9, "tier": "hot"},
    ])
    assert len(recs) == 1 and recs[0].dataset_id == "cold"
