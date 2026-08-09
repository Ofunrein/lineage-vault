from __future__ import annotations

import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

from ..engine import LineageVaultEngine
from ..openlineage.models import OpenLineageRunEvent


@dataclass
class BenchmarkResult:
    events: int
    workers: int
    duration_seconds: float
    throughput_eps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    peak_rss_mb: float
    recovery_seconds: float
    acknowledged_writes: int
    integrity_ok: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _make_event(i: int) -> OpenLineageRunEvent:
    run_id = f"bench-{i}"
    now = datetime.now(timezone.utc)
    return OpenLineageRunEvent(
        eventType="COMPLETE",
        eventTime=now,
        run={"runId": run_id},
        job={"namespace": "bench", "name": "load"},
        inputs=[
            {
                "namespace": "bench",
                "name": f"src_{i % 50}",
                "facets": {"schema": {"fields": [{"name": "id", "type": "integer"}]}},
            }
        ],
        outputs=[
            {
                "namespace": "bench",
                "name": f"dst_{i % 50}",
                "facets": {"schema": {"fields": [{"name": "id", "type": "integer"}]}},
            }
        ],
    )


def run_benchmark(
    *,
    data_dir: str | Path,
    events: int = 10_000,
    workers: int = 8,
    output: str | Path | None = None,
) -> BenchmarkResult:
    data_dir = Path(data_dir)
    if data_dir.exists():
        for p in data_dir.glob("*.db"):
            p.unlink()

    proc = psutil.Process() if psutil else None
    mem_start = proc.memory_info().rss if proc else 0
    peak_rss = mem_start
    latencies: list[float] = []

    eng = LineageVaultEngine(data_dir)
    eng.recover()

    def ingest_one(i: int) -> float:
        nonlocal peak_rss
        t0 = time.perf_counter()
        ev = _make_event(i)
        eng.openlineage.ingest(ev, idempotency_key=f"bench:{i}")
        dt = (time.perf_counter() - t0) * 1000
        if proc:
            peak_rss = max(peak_rss, proc.memory_info().rss)
        return dt

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(ingest_one, i) for i in range(events)]
        for fut in as_completed(futures):
            latencies.append(fut.result())
    duration = time.perf_counter() - t_start

    t_rec = time.perf_counter()
    recovered = eng.recover()
    recovery_seconds = time.perf_counter() - t_rec
    acknowledged = eng.storage.count_acknowledged()
    integrity = eng.verify()
    eng.close()

    latencies.sort()
    p50 = latencies[len(latencies) // 2] if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0.0
    peak_mb = (peak_rss - mem_start) / (1024 * 1024) if proc else 0.0

    result = BenchmarkResult(
        events=events,
        workers=workers,
        duration_seconds=round(duration, 3),
        throughput_eps=round(events / duration, 2) if duration else 0.0,
        p50_ms=round(p50, 3),
        p95_ms=round(p95, 3),
        p99_ms=round(p99, 3),
        peak_rss_mb=round(peak_mb, 2),
        recovery_seconds=round(recovery_seconds, 3),
        acknowledged_writes=acknowledged,
        integrity_ok=integrity,
    )

    if output:
        Path(output).write_text(json.dumps(result.to_dict(), indent=2) + "\n")
    return result
