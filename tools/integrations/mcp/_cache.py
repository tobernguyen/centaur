"""Sandbox-local disk cache: session state, tool inventories, denial markers.

Every bridge invocation is a fresh subprocess, so without this cache each call
to a stateful server would pay the 3-request MCP handshake again. Sandboxes are
per-thread and never recycled across threads, so a plain per-sandbox path is
thread-scoped by construction. Best-effort: any cache failure degrades to extra
HTTP requests, never to an error.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

ROOT = Path.home() / ".cache" / "centaur-mcp"

TOOLS_TTL_SECONDS = 300.0
DENIAL_TTL_SECONDS = 300.0


def _path(server: str, name: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", server)
    return ROOT / safe / name


def read(server: str, name: str, ttl: float | None = None) -> Any | None:
    try:
        path = _path(server, name)
        if ttl is not None and time.time() - path.stat().st_mtime > ttl:
            return None
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def write(server: str, name: str, data: Any) -> None:
    try:
        path = _path(server, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))
    except OSError:
        pass


def invalidate(server: str, name: str) -> None:
    try:
        _path(server, name).unlink(missing_ok=True)
    except OSError:
        pass
