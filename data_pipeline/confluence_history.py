"""
Persistent confluence journal for Watch Tower.

Tracks every valid confluence setup discovered by the strategy,
including setups that:

- wait for entry
- expire without reaching the order-block zone
- are rejected because of position-sizing constraints
- become executed trades
- eventually win, lose, or break even

This is intentionally separate from trade_history.py.

Confluence history answers:
"What setups did Watch Tower find?"

Trade history answers:
"What trades did Watch Tower actually place?"
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import db_sink
from instance_config import history_path

HISTORY_FILE = Path(__file__).with_name("confluence_history.csv")


def _history_file() -> Path:
    return history_path("confluence_history.csv", HISTORY_FILE)


FIELDS = [
    "signal_time",
    "symbol",
    "timeframe",
    "direction",
    "score",

    # Confluence components
    "h1_bias",
    "liquidity_sweep",
    "m15_structure",
    "m5_fvg",
    "m5_order_block",

    # Setup / zone information
    "ob_lower",
    "ob_upper",
    "setup_id",

    # Lifecycle
    "status",
    "rejection_reason",

    # Trade information, when applicable
    "entry",
    "stop_loss",
    "take_profit",
    "contract_id",
    "outcome",
    "profit",

    # Useful timestamps
    "entry_time",
    "exit_time",
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


def record_confluence(**values: Any) -> int:
    """
    Append a confluence setup and return its zero-based row index.
    """

    history_file = _history_file()
    _ensure_file(history_file)

    row = {field: "" for field in FIELDS}

    for key in FIELDS:
        if key in values:
            row[key] = values[key]

    if not row["signal_time"]:
        row["signal_time"] = _now()

    with history_file.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FIELDS,
        )
        writer.writerow(row)

    index = _row_count(history_file) - 1
    # The CSV is written first and stays the source of truth. Mirroring into
    # the database is best effort and never raises.
    db_sink.record_signal(index, row)
    return index


def update_confluence(
    row_index: int,
    **values: Any,
) -> None:
    """
    Update an existing confluence journal row.
    """

    history_file = _history_file()
    _ensure_file(history_file)

    with history_file.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as handle:
        rows = list(csv.DictReader(handle))

    if row_index < 0 or row_index >= len(rows):
        raise IndexError(
            f"Confluence history row {row_index} does not exist"
        )

    for key, value in values.items():
        if key in FIELDS:
            rows[row_index][key] = value

    temp_file = history_file.with_suffix(".tmp")

    with temp_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerows(rows)

    temp_file.replace(history_file)

    db_sink.update_signal(row_index, values)


def _ensure_file(history_file: Path | None = None) -> None:
    history_file = history_file or _history_file()
    history_file.parent.mkdir(parents=True, exist_ok=True)
    if history_file.exists():
        _upgrade_header(history_file)
        return

    with history_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FIELDS,
        )
        writer.writeheader()


def _upgrade_header(history_file: Path | None = None) -> None:
    """Bring an older journal up to the current columns, keeping every row.

    A column added to FIELDS would otherwise misalign against a file
    written with the previous header, so the file is rewritten once with
    the new columns and the existing rows left untouched apart from the
    new column being empty.
    """
    history_file = history_file or _history_file()
    with history_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing = reader.fieldnames or []
        if existing == FIELDS:
            return
        rows = list(reader)

    if set(existing) - set(FIELDS):
        # The file has columns this version does not know about. Leave it
        # alone rather than dropping data that something else may need.
        return

    temp_file = history_file.with_suffix(".upgrade")
    with temp_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})
    temp_file.replace(history_file)


def _row_count(history_file: Path | None = None) -> int:
    history_file = history_file or _history_file()
    if not history_file.exists():
        return 0

    with history_file.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as handle:
        return sum(
            1
            for _ in csv.DictReader(handle)
        )
