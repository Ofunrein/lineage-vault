from __future__ import annotations

import hashlib
import json
from typing import Any

GENESIS_HASH = "0" * 64


def payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def entry_hash(prev: str, event_id: str, payload: dict[str, Any] | str) -> str:
    payload_text = payload if isinstance(payload, str) else payload_json(payload)
    return hashlib.sha256(f"{prev}|{event_id}|{payload_text}".encode()).hexdigest()
