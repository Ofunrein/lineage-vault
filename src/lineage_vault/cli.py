from __future__ import annotations

import argparse
import json
import sys

import httpx
import uvicorn

from .api.app import create_app
from .benchmark.harness import run_benchmark, run_comparative_benchmark
from .demo.pipeline import run_demo_pipeline
from .engine import LineageVaultEngine
from .observability.metrics import configure_logging
from .storage.config import load_storage_config


def main() -> None:
    configure_logging()
    p = argparse.ArgumentParser(prog="lineage-vault")
    sub = p.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve")
    serve.add_argument("--data-dir", default=".data")
    serve.add_argument("--backend", choices=["sqlite", "postgres"], default=None)

    v = sub.add_parser("verify")
    v.add_argument("--data-dir", default=".data")

    demo = sub.add_parser("demo")
    demo.add_argument("--data-dir", default=".data-demo")

    bench = sub.add_parser("benchmark")
    bench.add_argument("--events", type=int, default=10_000)
    bench.add_argument("--workers", type=int, default=8)
    bench.add_argument("--batch-size", type=int, default=50)
    bench.add_argument("--data-dir", default=".data-bench")
    bench.add_argument("--output", default="docs/benchmark-results.json")
    bench.add_argument(
        "--mode",
        choices=["sqlite-single", "sqlite-batch", "postgres-single", "postgres-batch", "compare"],
        default="sqlite-single",
    )

    wf = sub.add_parser("workflow")
    wf.add_argument("--base-url", default="http://127.0.0.1:8000")
    wf.add_argument("--data-dir", default=".data-workflow")

    args = p.parse_args()

    if args.cmd == "serve":
        config = load_storage_config(data_dir=args.data_dir)
        if args.backend:
            config = load_storage_config(data_dir=args.data_dir)
            from dataclasses import replace

            config = replace(config, backend=args.backend)
        uvicorn.run(create_app(args.data_dir, config=config), host="0.0.0.0", port=8000)
    elif args.cmd == "verify":
        eng = LineageVaultEngine(args.data_dir)
        eng.recover()
        ok = eng.verify()
        print("integrity:", ok)
        print("acknowledged:", eng.storage.count_acknowledged())
        print("backend:", eng.config.backend)
        eng.close()
        sys.exit(0 if ok else 1)
    elif args.cmd == "demo":
        eng = LineageVaultEngine(args.data_dir)
        eng.recover()
        result = run_demo_pipeline(eng)
        print(json.dumps(result, indent=2, default=str))
        eng.close()
    elif args.cmd == "benchmark":
        if args.mode == "compare":
            payload = run_comparative_benchmark(
                data_dir=args.data_dir,
                events=args.events,
                workers=args.workers,
                batch_size=args.batch_size,
                output=args.output,
            )
            print(json.dumps(payload, indent=2))
        else:
            result = run_benchmark(
                data_dir=args.data_dir,
                events=args.events,
                workers=args.workers,
                batch_size=args.batch_size,
                mode=args.mode,
                output=args.output,
            )
            print(json.dumps(result.to_dict(), indent=2))
    elif args.cmd == "workflow":
        _run_workflow(args.base_url, args.data_dir)


def _run_workflow(base_url: str, data_dir: str) -> None:
    """G5 demo: init, ingest, impact query, verify, health — <=5 commands."""
    eng = LineageVaultEngine(data_dir)
    eng.recover()
    run_demo_pipeline(eng)
    eng.close()

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        health = client.get("/health")
        ready = client.get("/ready")
        impact = client.get("/impact/field/warehouse.raw_orders/amount")
        verify = client.post("/verify")
    print(json.dumps({
        "health": health.json(),
        "ready": ready.json(),
        "impact": impact.json(),
        "verify": verify.json(),
    }, indent=2))


if __name__ == "__main__":
    main()
