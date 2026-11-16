"""Best-effort, bounded operational events and runtime snapshots.

This module is deliberately passive: callers enqueue records without waiting
for disk or PostgreSQL I/O. A daemon worker writes a bounded JSONL fallback
and mirrors records to PostgreSQL when enabled.
"""

from __future__ import annotations

import json
import os
import queue
import re
import sys
import threading
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


_QUEUE: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(maxsize=2000)
_WORKER: threading.Thread | None = None
_LOCK = threading.Lock()
_DROPPED = 0
_HANDLERS: dict[str, RotatingFileHandler] = {}
_MAX_BYTES = 1_000_000
_BACKUP_COUNT = 2

_SENSITIVE_KEY = re.compile(r"token|password|secret|credential|private.?key|authorization", re.I)
_SENSITIVE_TEXT = re.compile(
    r"(?i)(DERIV_API_TOKEN|POSTGRES_PASSWORD|DATABASE_PASSWORD|authorization|token|password|secret)\s*[:=]\s*([^\s,;]+)"
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(value: Any, key: str = "") -> Any:
    if _SENSITIVE_KEY.search(key):
        return "***"
    if isinstance(value, dict):
        return {str(k): _safe(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    if isinstance(value, str):
        value = _BEARER.sub("Bearer ***", value)
        return _SENSITIVE_TEXT.sub(r"\1=***", value)[:4000]
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return str(value)[:4000]


def _file_handler(component: str) -> RotatingFileHandler | None:
    base = os.environ.get("DATA_DIR", "").strip()
    if not base:
        return None
    role_part = re.sub(r"[^A-Za-z0-9_-]", "_", os.environ.get("INSTANCE_PROCESS", "trader"))[:16] or "trader"
    component_part = re.sub(r"[^A-Za-z0-9_-]", "_", component)[:32] or "general"
    safe_component = f"{role_part}.{component_part}"
    with _LOCK:
        handler = _HANDLERS.get(safe_component)
        if handler is not None:
            return handler
        directory = Path(base) / "ops"
        try:
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            path = directory / f"operational_events.{safe_component}.jsonl"
            handler = RotatingFileHandler(
                path, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT,
                encoding="utf-8", delay=True,
            )
            try:
                os.chmod(path, 0o600)
            except FileNotFoundError:
                pass
            _HANDLERS[safe_component] = handler
            return handler
        except Exception as exc:
            print(f"[watchtower-observability] local event write unavailable ({type(exc).__name__})", file=sys.stderr, flush=True)
            return None


def _write_event_file(event: dict[str, Any]) -> None:
    handler = _file_handler(str(event.get("component") or "general"))
    if handler is None:
        return
    try:
        # One JSON object per physical line keeps the fallback easy to inspect.
        import logging
        record = logging.LogRecord("watchtower.ops", logging.INFO, "", 0, json.dumps(event, separators=(",", ":"), ensure_ascii=False), (), None)
        handler.emit(record)
        if handler.baseFilename and os.path.exists(handler.baseFilename):
            os.chmod(handler.baseFilename, 0o600)
    except Exception as exc:
        print(f"[watchtower-observability] event append failed ({type(exc).__name__})", file=sys.stderr, flush=True)


def _worker() -> None:
    while True:
        action, payload = _QUEUE.get()
        try:
            if action == "event":
                _write_event_file(payload)
                try:
                    import db_sink
                    db_sink.record_operational_event(payload)
                except Exception as exc:
                    print(f"[watchtower-observability] database event mirror failed ({type(exc).__name__})", file=sys.stderr, flush=True)
            elif action == "runtime":
                try:
                    import db_sink
                    db_sink.record_runtime_status(payload["component"], payload["state"])
                except Exception as exc:
                    print(f"[watchtower-observability] runtime mirror failed ({type(exc).__name__})", file=sys.stderr, flush=True)
        finally:
            _QUEUE.task_done()


def _ensure_worker() -> None:
    global _WORKER
    with _LOCK:
        if _WORKER is None or not _WORKER.is_alive():
            _WORKER = threading.Thread(target=_worker, name="watchtower-observability", daemon=True)
            _WORKER.start()


def _enqueue(action: str, payload: dict[str, Any]) -> bool:
    global _DROPPED
    try:
        _ensure_worker()
        _QUEUE.put_nowait((action, payload))
        return True
    except queue.Full:
        _DROPPED += 1
        if _DROPPED == 1 or _DROPPED % 100 == 0:
            print("[watchtower-observability] bounded queue full; operational record dropped", file=sys.stderr, flush=True)
        return False
    except Exception as exc:
        print(f"[watchtower-observability] enqueue failed ({type(exc).__name__})", file=sys.stderr, flush=True)
        return False


def emit_event(
    event_type: str,
    *,
    severity: str = "INFO",
    component: str = "trader",
    symbol: str | None = None,
    timeframe: str | None = None,
    signal_id: str | None = None,
    trade_id: str | None = None,
    contract_id: str | None = None,
    message: str = "",
    reason: str | None = None,
    details: dict[str, Any] | None = None,
) -> bool:
    """Queue a structured event without blocking its caller."""
    event = _safe({
        "event_id": str(uuid.uuid4()),
        "occurred_at": _now(),
        "instance_id": os.environ.get("INSTANCE_ID", "unconfigured"),
        "severity": severity.upper() if severity.upper() in {"INFO", "WARN", "ERROR"} else "INFO",
        "component": component,
        "event_type": event_type,
        "symbol": symbol,
        "timeframe": str(timeframe) if timeframe is not None else None,
        "signal_id": signal_id,
        "trade_id": trade_id,
        "contract_id": str(contract_id) if contract_id is not None else None,
        "message": message,
        "reason": reason,
        "details": details or {},
    })
    return _enqueue("event", event)


def emit_runtime_status(component: str, state: dict[str, Any]) -> bool:
    """Queue a complete component snapshot; values are redacted before storage."""
    return _enqueue("runtime", {"component": component, "state": _safe(state)})


def flush(timeout: float = 2.0) -> bool:
    """Best-effort test/shutdown helper; never used on a trade decision path."""
    import time
    deadline = time.monotonic() + max(0.0, timeout)
    while _QUEUE.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.01)
    return _QUEUE.unfinished_tasks == 0
