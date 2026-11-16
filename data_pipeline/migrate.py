"""Apply pending SQL migrations, and optionally import the existing CSVs.

Migrations are numbered files in migrations/. Each one is applied once and
recorded in schema_migrations, so running this repeatedly is safe and no
existing data is ever dropped or replaced.

    python migrate.py            apply pending migrations
    python migrate.py --backfill also import rows already in the CSV
                                 journals, skipping any that are present
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import psycopg
from instance_config import load_instance_environment, selected_data_dir

MIGRATIONS = Path(__file__).with_name("migrations")
INSTANCE_ID = load_instance_environment()


def dsn() -> str:
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    user = os.environ.get("POSTGRES_USER", "").strip()
    password = os.environ.get("POSTGRES_PASSWORD", "").strip()
    if not user or not password:
        sys.exit("POSTGRES_USER and POSTGRES_PASSWORD (or DATABASE_URL) must be set")
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'watchtower')} "
        f"user={user} password={password}"
    )


def apply_migrations(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "  name TEXT PRIMARY KEY,"
            "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        cursor.execute("SELECT name FROM schema_migrations")
        done = {row[0] for row in cursor.fetchall()}

    for path in sorted(MIGRATIONS.glob("*.sql")):
        if path.name in done:
            print(f"  {path.name}: already applied")
            continue
        print(f"  {path.name}: applying")
        with connection.cursor() as cursor:
            cursor.execute(path.read_text(encoding="utf-8"))
            cursor.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))


def _value(raw: str):
    """CSV blanks become NULL; numbers become numbers where they parse."""
    text = (raw or "").strip()
    return text or None


def _number(raw: str):
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _is_header_row(row: dict) -> bool:
    """True for a header line repeated inside the data.

    Both journals contain one, left by an earlier run that rewrote the
    header mid-file. The CSV is the user's record and is not edited here;
    the row is simply not imported.
    """
    return (row.get("signal_time") or "").strip() == "signal_time"


def backfill(connection) -> None:
    """Import rows already sitting in the CSV journals. Idempotent."""
    data_dir = selected_data_dir()

    signals_csv = data_dir / "confluence_history.csv"
    if signals_csv.is_file():
        with signals_csv.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        for index, row in enumerate(rows):
            if _is_header_row(row):
                continue
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO signals (
                        csv_row, signal_time, symbol, timeframe, direction, score,
                        h1_bias, liquidity_sweep, m15_structure, m5_fvg, m5_order_block,
                        ob_lower, ob_upper, setup_id, status, rejection_reason,
                        entry, stop_loss, take_profit, contract_id, outcome, profit,
                        entry_time, exit_time
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s, %s,%s
                    ) ON CONFLICT (csv_row) DO NOTHING
                    """,
                    (
                        index, _value(row.get("signal_time")), _value(row.get("symbol")),
                        _value(row.get("timeframe")), _value(row.get("direction")),
                        _number(row.get("score")),
                        _value(row.get("h1_bias")), _value(row.get("liquidity_sweep")),
                        _value(row.get("m15_structure")), _value(row.get("m5_fvg")),
                        _value(row.get("m5_order_block")),
                        _number(row.get("ob_lower")), _number(row.get("ob_upper")),
                        _value(row.get("setup_id")), _value(row.get("status")),
                        _value(row.get("rejection_reason")),
                        _number(row.get("entry")), _number(row.get("stop_loss")),
                        _number(row.get("take_profit")), _value(row.get("contract_id")),
                        _value(row.get("outcome")), _number(row.get("profit")),
                        _value(row.get("entry_time")), _value(row.get("exit_time")),
                    ),
                )
        print(f"  confluence_history.csv: {len(rows)} row(s) considered")

    trades_csv = data_dir / "trade_history.csv"
    if trades_csv.is_file():
        with trades_csv.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        for index, row in enumerate(rows):
            if _is_header_row(row):
                continue
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO trades (
                        csv_row, signal_time, symbol, direction, score, entry,
                        stop_loss_price, take_profit_price, stop_distance_pct,
                        stake, multiplier, risk_amount, reward_amount,
                        contract_id, transaction_id, status, exit_time, profit, error,
                        entry_price, technical_sl_price, technical_tp_price,
                        technical_stop_distance, technical_reward_distance, technical_rr,
                        deriv_stop_loss_amount, deriv_take_profit_amount,
                        broker_stop_loss_value, broker_take_profit_value,
                        contract_created_at, contract_updated_at, final_pl
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s
                    ) ON CONFLICT (csv_row) DO NOTHING
                    """,
                    (
                        index, _value(row.get("signal_time")), _value(row.get("symbol")),
                        _value(row.get("direction")), _number(row.get("score")),
                        _number(row.get("entry")),
                        _number(row.get("stop_loss_price")), _number(row.get("take_profit_price")),
                        _number(row.get("stop_distance_pct")),
                        _number(row.get("stake")), _number(row.get("multiplier")),
                        _number(row.get("risk_amount")), _number(row.get("reward_amount")),
                        _value(row.get("contract_id")), _value(row.get("transaction_id")),
                        _value(row.get("status")), _value(row.get("exit_time")),
                        _number(row.get("profit")), _value(row.get("error")),
                        _number(row.get("entry_price")),
                        _number(row.get("technical_sl_price")),
                        _number(row.get("technical_tp_price")),
                        _number(row.get("technical_stop_distance")),
                        _number(row.get("technical_reward_distance")),
                        _number(row.get("technical_rr")),
                        _number(row.get("deriv_stop_loss_amount")),
                        _number(row.get("deriv_take_profit_amount")),
                        _number(row.get("broker_stop_loss_value")),
                        _number(row.get("broker_take_profit_value")),
                        _value(row.get("contract_created_at")),
                        _value(row.get("contract_updated_at")),
                        _number(row.get("final_pl")),
                    ),
                )
        print(f"  trade_history.csv: {len(rows)} row(s) considered")


def main() -> None:
    with psycopg.connect(dsn(), autocommit=True, connect_timeout=10) as connection:
        print("migrations:")
        apply_migrations(connection)
        if "--backfill" in sys.argv:
            print("backfill:")
            backfill(connection)
    print("done")


if __name__ == "__main__":
    main()
