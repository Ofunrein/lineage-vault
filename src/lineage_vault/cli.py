from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import httpx
import uvicorn

from .api.app import create_app
from .benchmark.harness import run_benchmark
from .demo.pipeline import run_demo_pipeline
from .engine import LineageVaultEngine
from .observability.metrics import configure_logging


def main() -> None:
    configure_logging()
    p = argparse.ArgumentParser(prog="lineage-vault")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("serve")
    v = sub.add_parser("verify")
    v.add_argument("--data-dir", default=".data")

    demo = sub.add_parser("demo")
    demo.add_argument("--data-dir", default=".data-demo")

    bench = sub.add_parser("benchmark")
    bench.add_argument("--events", type=int, default=10_000)
    bench.add_argument("--workers", type=int, default=8)
    bench.add_argument("--data-dir", default=".data-bench")
    bench.add_argument("--output", default="docs/benchmark-results.json")

    wf = sub.add_parser("workflow")
    wf.add_argument("--base-url", default="http://127.0.0.1:8000")
    wf.add_argument("--data-dir", default=".data-workflow")

    args = p.parse_args()

    if args.cmd == "serve":
        uvicorn.run(create_app(), host="0.0.0.0", port=8000)
    elif args.cmd == "verify":
        eng = LineageVaultEngine(args.data_dir)
        eng.recover()
        ok = eng.verify()
        print("integrity:", ok)
        print("acknowledged:", eng.storage.count_acknowledged())
        eng.close()
        sys.exit(0 if ok else 1)
    elif args.cmd == "demo":
        eng = LineageVaultEngine(args.data_dir)
        eng.recover()
        result = run_demo_pipeline(eng)
        print(json.dumps(result, indent=2, default=str))
        eng.close()
    elif args.cmd == "benchmark":
        result = run_benchmark(
            data_dir=args.data_dir,
            events=args.events,
            workers=args.workers,
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
    impact = client.get("/impact/field/warehouse.raw_orders/amount")
    verify = client.post("/verify")
  print(json.dumps({
    "health": health.json(),
    "impact": impact.json(),
    "verify": verify.json(),
  }, indent=2))


if __name__ == "__main__":
    main()
