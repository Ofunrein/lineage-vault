from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

from ..engine import LineageVaultEngine
from ..openlineage.models import OpenLineageRunEvent
from ..storage.config import StorageConfig, load_storage_config
from ..storage.factory import postgres_available
from ..storage.interface import BatchWriteItem

BenchmarkMode = Literal["sqlite-single", "sqlite-batch", "postgres-single", "postgres-batch"]


@dataclass
class BenchmarkResult:
    mode: str
    backend: str
    events: int
    workers: int
    batch_size: int
    duration_seconds: float
    throughput_eps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    peak_rss_mb: float
    recovery_seconds: float
    acknowledged_writes: int
    integrity_ok: bool
    skipped: bool = False
    skip_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _make_event(i: int) -> OpenLineageRunEvent:
    run_id = f"bench-{i}"
    now = datetime.now(UTC)
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


def _cleanup_data_dir(data_dir: Path, backend: str) -> None:
    if backend == "sqlite" and data_dir.exists():
        for p in data_dir.glob("*.db"):
            p.unlink()
    elif backend == "postgres":
        return


def _run_mode(
    *,
    mode: BenchmarkMode,
    data_dir: Path,
    events: int,
    workers: int,
    batch_size: int,
) -> BenchmarkResult:
    if mode.startswith("postgres"):
        config = load_storage_config(data_dir=data_dir)
        config = StorageConfig(
            backend="postgres",
            data_dir=data_dir,
            sqlite_path=config.sqlite_path,
            postgres_dsn=config.postgres_dsn,
        )
        if not config.postgres_dsn or not postgres_available(config.postgres_dsn):
            return BenchmarkResult(
                mode=mode,
                backend="postgres",
                events=events,
                workers=workers,
                batch_size=batch_size,
                duration_seconds=0.0,
                throughput_eps=0.0,
                p50_ms=0.0,
                p95_ms=0.0,
                p99_ms=0.0,
                peak_rss_mb=0.0,
                recovery_seconds=0.0,
                acknowledged_writes=0,
                integrity_ok=False,
                skipped=True,
                skip_reason="postgres unavailable",
            )
    else:
        config = StorageConfig(
            backend="sqlite",
            data_dir=data_dir,
            sqlite_path=data_dir / "vault.db",
        )

    _cleanup_data_dir(data_dir, config.backend)
    proc = psutil.Process() if psutil else None
    mem_start = proc.memory_info().rss if proc else 0
    peak_rss = mem_start
    latencies: list[float] = []

    eng = LineageVaultEngine(data_dir, config=config)
    eng.recover()
    use_batch = mode.endswith("-batch")

    def ingest_one(i: int) -> float:
        nonlocal peak_rss
        t0 = time.perf_counter()
        ev = _make_event(i)
        if use_batch:
            items = [
                BatchWriteItem(
                    idempotency_key=f"bench:{i}",
                    event_id=str(uuid4()),
                    payload=ev.model_dump(mode="json"),
                )
            ]
            eng.storage.acknowledge_writes_batch(items)
        else:
            eng.openlineage.ingest(ev, idempotency_key=f"bench:{i}")
        dt = (time.perf_counter() - t0) * 1000
        if proc:
            peak_rss = max(peak_rss, proc.memory_info().rss)
        return dt

    def ingest_batch_chunk(start: int, chunk: list[int]) -> list[float]:
        nonlocal peak_rss
        t0 = time.perf_counter()
        items = []
        for i in chunk:
            ev = _make_event(i)
            items.append(
                BatchWriteItem(
                    idempotency_key=f"bench:{i}",
                    event_id=str(uuid4()),
                    payload=ev.model_dump(mode="json"),
                )
            )
        eng.storage.acknowledge_writes_batch(items)
        dt = (time.perf_counter() - t0) * 1000
        per_item = dt / len(chunk) if chunk else 0.0
        if proc:
            peak_rss = max(peak_rss, proc.memory_info().rss)
        return [per_item] * len(chunk)

    t_start = time.perf_counter()
    if use_batch and batch_size > 1:
        indices = list(range(events))
        chunks = [indices[i : i + batch_size] for i in range(0, events, batch_size)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(ingest_batch_chunk, c[0], c) for c in chunks]
            for fut in as_completed(futures):
                latencies.extend(fut.result())
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(ingest_one, i) for i in range(events)]
            for fut in as_completed(futures):
                latencies.append(fut.result())
    duration = time.perf_counter() - t_start

    t_rec = time.perf_counter()
    eng.recover()
    recovery_seconds = time.perf_counter() - t_rec
    acknowledged = eng.storage.count_acknowledged()
    integrity = eng.verify()
    eng.close()

    latencies.sort()
    p50 = latencies[len(latencies) // 2] if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0.0
    peak_mb = (peak_rss - mem_start) / (1024 * 1024) if proc else 0.0

    return BenchmarkResult(
        mode=mode,
        backend=config.backend,
        events=events,
        workers=workers,
        batch_size=batch_size if use_batch else 1,
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


def run_benchmark(
    *,
    data_dir: str | Path,
    events: int = 10_000,
    workers: int = 8,
    batch_size: int = 50,
    mode: BenchmarkMode = "sqlite-single",
    output: str | Path | None = None,
) -> BenchmarkResult:
    result = _run_mode(
        mode=mode,
        data_dir=Path(data_dir),
        events=events,
        workers=workers,
        batch_size=batch_size,
    )
    if output:
        Path(output).write_text(json.dumps(result.to_dict(), indent=2) + "\n")
    return result


def run_comparative_benchmark(
    *,
    data_dir: str | Path,
    events: int = 10_000,
    workers: int = 8,
    batch_size: int = 50,
    output: str | Path | None = None,
) -> dict[str, Any]:
    base = Path(data_dir)
    modes: list[BenchmarkMode] = [
        "sqlite-single",
        "sqlite-batch",
        "postgres-single",
        "postgres-batch",
    ]
    results: dict[str, Any] = {}
    for mode in modes:
        mode_dir = base / mode.replace("-", "_")
        mode_dir.mkdir(parents=True, exist_ok=True)
        results[mode] = _run_mode(
            mode=mode,
            data_dir=mode_dir,
            events=events,
            workers=workers,
            batch_size=batch_size,
        ).to_dict()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "events": events,
        "workers": workers,
        "batch_size": batch_size,
        "results": results,
    }
    if output:
        Path(output).write_text(json.dumps(payload, indent=2) + "\n")
    return payload
