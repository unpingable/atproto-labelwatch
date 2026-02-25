from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def hash_sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def sqlite_safe_text(value: Any) -> Optional[str]:
    """Coerce any value to a type sqlite3 can bind as TEXT, or None."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def get_git_commit() -> Optional[str]:
    try:
        head_path = os.path.join(os.getcwd(), ".git", "HEAD")
        if not os.path.exists(head_path):
            return None
        with open(head_path, "r", encoding="utf-8") as f:
            ref = f.read().strip()
        if ref.startswith("ref:"):
            ref_path = ref.split(" ", 1)[1].strip()
            full_ref = os.path.join(os.getcwd(), ".git", ref_path)
            if os.path.exists(full_ref):
                with open(full_ref, "r", encoding="utf-8") as f:
                    return f.read().strip() or None
            return None
        return ref or None
    except OSError:
        return None
