"""Gauntlet L4 — 20 schema fixtures, critic accuracy."""
import json
from pathlib import Path

from lineage_vault.schema.engine import SchemaEngine


def test_gauntlet_l4_all_fixtures():
    fixtures = json.loads((Path(__file__).parent / "schema_fixtures.json").read_text())
    engine = SchemaEngine()
    assert len(fixtures) == 20
    for i, fx in enumerate(fixtures):
        got = engine.evaluate(fx["old"], fx["new"]).value
        assert got == fx["expect"], f"fixture {i}: got {got}, want {fx['expect']}"
