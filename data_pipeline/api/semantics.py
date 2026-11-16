"""Pure helpers for dashboard/API row meaning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def redact_account_id(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    return f"…{text[-3:]}" if len(text) > 3 else "REDACTED"


def active_signals_from_runtime_rows(rows: list[dict], *, now: datetime | None = None, freshness_seconds: int = 75) -> list[dict]:
    """Only fresh runtime snapshots with an actual in-memory pending signal qualify."""
    now = now or datetime.now(timezone.utc)
    items = []
    for runtime in rows:
        if not str(runtime.get("component", "")).startswith("trader:"):
            continue
        updated_at = runtime.get("updated_at")
        if not updated_at or (now - updated_at).total_seconds() > freshness_seconds:
            continue
        state = runtime.get("state") or {}
        pending = state.get("pending_signal")
        if not state.get("running") or not pending:
            continue
        items.append({
            **pending,
            "symbol": state.get("symbol"),
            "status": "PENDING",
            "signal_time": pending.get("signal_time") or updated_at,
        })
    return sorted(items, key=lambda item: str(item.get("signal_time", "")), reverse=True)


def actual_trade_where(where: str) -> str:
    """Limit trade history/performance to rows with a broker contract ID."""
    extra = where.replace(" WHERE ", " AND ", 1) if where else ""
    return " WHERE contract_id IS NOT NULL" + extra
