from __future__ import annotations
import argparse, uvicorn
from .api.app import create_app
from .engine import LineageVaultEngine

def main() -> None:
    p = argparse.ArgumentParser(prog="lineage-vault")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    sub.add_parser("verify")
    args = p.parse_args()
    if args.cmd == "serve":
        uvicorn.run(create_app(), host="0.0.0.0", port=8000)
    elif args.cmd == "verify":
        e = LineageVaultEngine()
        e.recover()
        print("integrity:", e.verify())
        e.close()

if __name__ == "__main__":
    main()
