from __future__ import annotations

from ..storage.interface import StorageBackend


class ImpactAnalyzer:
    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    def dataset_impact(self, dataset: str) -> dict:
        return {
            "dataset": dataset,
            "downstream_datasets": self._storage.downstream_datasets(dataset),
            "upstream_datasets": self._storage.upstream_datasets(dataset),
        }

    def field_impact(self, dataset: str, field: str) -> dict:
        return self._storage.field_impact(dataset, field)
