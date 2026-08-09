from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class TierRecommendation:
    dataset_id: str
    access_count: int
    bytes_stored: int
    current_tier: str
    recommended_tier: str
    reason: str

class LifecycleOptimizer:
    COLD_THRESHOLD = 10

    def analyze(self, telemetry: list[dict[str, Any]]) -> list[TierRecommendation]:
        recs = []
        for row in telemetry:
            accesses = int(row.get("access_count", 0))
            if accesses < self.COLD_THRESHOLD and row.get("tier", "hot") == "hot":
                recs.append(TierRecommendation(
                    dataset_id=row["dataset_id"], access_count=accesses,
                    bytes_stored=int(row.get("bytes", 0)), current_tier="hot",
                    recommended_tier="cold", reason=f"access_count={accesses} below threshold",
                ))
        return recs
