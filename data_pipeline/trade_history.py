"""Persistent local trade journal for Watch Tower demo execution."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import db_sink
from instance_config import history_path

HISTORY_FILE = Path(__file__).with_name("trade_history.csv")


def _history_file() -> Path:
    return history_path("trade_history.csv", HISTORY_FILE)

FIELDS = [
    "signal_time",
    "symbol",
    "direction",
    "score",
    "entry",
    "stop_loss_price",
    "take_profit_price",
    "stop_distance_pct",
    "stake",
    "multiplier",
    "risk_amount",
    "reward_amount",
    "contract_id",
    "transaction_id",
    "status",
    "exit_time",
    "profit",
    "error",
    # Explicitly named observability fields. Legacy columns above remain
    # unchanged so existing CSV/API consumers keep their current contract.
    "entry_price",
    "technical_sl_price",
    "technical_tp_price",
    "technical_stop_distance",
    "technical_reward_distance",
    "technical_rr",
    "deriv_stop_loss_amount",
    "deriv_take_profit_amount",
    "broker_stop_loss_value",
    "broker_take_profit_value",
    "contract_created_at",
    "contract_updated_at",
    "final_pl",
    "quality_score",
    "quality_category",
    "quality_components",
    "displacement_assessment",
    "premium_discount_state",
    "tp_source",
    "structural_target",
    "potential_rr",
    "setup_invalidation_reason",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_trade(**values: Any) -> int:
    """Append a trade/event row and return its zero-based row index."""
    history_file = _history_file()
    _ensure_file(history_file)
    row = {field: "" for field in FIELDS}
    row.update({key: values.get(key, "") for key in FIELDS})
    if not row["signal_time"]:
        row["signal_time"] = _now()

    with history_file.open("r", newline="", encoding="utf-8") as handle:
        fieldnames = csv.DictReader(handle).fieldnames or FIELDS
    with history_file.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writerow(row)

    index = _row_count(history_file) - 1
    # The CSV is written first and stays the source of truth. Mirroring into
    # the database is best effort and never raises, so a database problem
    # cannot interrupt trading.
    db_sink.record_trade_row(index, row)
    return index


def get_open_trade_rows(symbol: str | None = None) -> list[dict[str, Any]]:
    """Return persisted open trades with their CSV row indexes for recovery."""
    history_file = _history_file()
    if not history_file.exists():
        return []

    open_rows: list[dict[str, Any]] = []
    with history_file.open("r", newline="", encoding="utf-8") as handle:
        for row_index, row in enumerate(csv.DictReader(handle)):
            contract_id = (row.get("contract_id") or "").strip()
            row_symbol = (row.get("symbol") or "").strip()
            if (
                (row.get("status") or "").strip().upper() != "OPEN"
                or not contract_id
                or (symbol is not None and row_symbol != symbol)
            ):
                continue
            open_rows.append(
                {**row, "row_index": row_index, "contract_id": contract_id, "symbol": row_symbol}
            )
    return open_rows


def update_trade(row_index: int, **values: Any) -> None:
    """Update one existing journal row by its zero-based data-row index."""
    history_file = _history_file()
    _ensure_file(history_file)
    with history_file.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if row_index < 0 or row_index >= len(rows):
        raise IndexError(f"Trade history row {row_index} does not exist")

    for key, value in values.items():
        if key in FIELDS:
            rows[row_index][key] = value

    with history_file.open("r", newline="", encoding="utf-8") as handle:
        fieldnames = csv.DictReader(handle).fieldnames or FIELDS

    temp_file = history_file.with_suffix(".tmp")
    with temp_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    temp_file.replace(history_file)

    db_sink.update_trade_row(row_index, values)


def _ensure_file(history_file: Path | None = None) -> None:
    history_file = history_file or _history_file()
    history_file.parent.mkdir(parents=True, exist_ok=True)
    if not history_file.exists():
        with history_file.open("w", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=FIELDS).writeheader()
        return

    with history_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing_fields = reader.fieldnames or []
        if all(field in existing_fields for field in FIELDS):
            return
        rows = list(reader)

    # Extend old schemas in place on the next journal write. Keep existing
    # columns and all rows intact; unknown columns from older/future readers
    # are preserved after the canonical fields.
    fieldnames = existing_fields + [field for field in FIELDS if field not in existing_fields]
    temp_file = history_file.with_suffix(".tmp")
    with temp_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    temp_file.replace(history_file)


def _row_count(history_file: Path | None = None) -> int:
    history_file = history_file or _history_file()
    with history_file.open("r", newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))
