"""Formatting helpers for broker-provided trade observability fields."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


NOT_AVAILABLE = "NOT_AVAILABLE"


def broker_value(order: Any) -> Any:
    """Return Deriv's explicit value field; never derive a broker value."""
    if not isinstance(order, dict):
        return NOT_AVAILABLE
    value = order.get("value")
    if value is None or (isinstance(value, str) and not value.strip()):
        return NOT_AVAILABLE
    return value


def utc_timestamp(value: Any) -> str:
    """Normalize a broker epoch or ISO timestamp to UTC; otherwise mark absent."""
    if value is None or value == "" or value == NOT_AVAILABLE:
        return NOT_AVAILABLE
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        pass

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return NOT_AVAILABLE


def contract_created_at(buy: Any) -> str:
    """Use Deriv buy.purchase_time when present; do not infer creation time."""
    if not isinstance(buy, dict):
        return NOT_AVAILABLE
    return utc_timestamp(buy.get("purchase_time"))


def contract_updated_at(contract_update: Any) -> str:
    """Use the latest available stop/target order_date from Deriv."""
    if not isinstance(contract_update, dict):
        return NOT_AVAILABLE

    timestamps = []
    for name in ("stop_loss", "take_profit"):
        order = contract_update.get(name)
        if isinstance(order, dict) and order.get("order_date") is not None:
            timestamp = utc_timestamp(order.get("order_date"))
            if timestamp != NOT_AVAILABLE:
                timestamps.append(timestamp)
    return max(timestamps) if timestamps else NOT_AVAILABLE
