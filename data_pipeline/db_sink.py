"""Mirror Watch Tower's existing history rows into PostgreSQL.

The CSV journals remain the source of truth and are written first. This
module copies the same rows into a database so the dashboard can query
them. It adds no strategy behaviour and decides nothing: it records what
trade_history.py and confluence_history.py already produce.

The trading loop must never stop because a database is unreachable, so
every call here is best effort. A failure is logged once per outage and
then swallowed; trading and CSV writing carry on untouched.

Rows are keyed by the CSV row index the existing functions already return,
so an update lands on the same row in both places without the callers
needing to know anything new.
"""

from __future__ import annotations

import logging
import os
import threading
import json
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("watchtower.db")

# Set WATCHTOWER_DB_ENABLED=0 to run with CSV only (the pre-database
# behaviour), which is also what happens when no DSN is configured.
_ENABLED = os.environ.get("WATCHTOWER_DB_ENABLED", "1").strip() not in ("0", "false", "False", "")

_lock = threading.Lock()
_connection = None
_failed = False  # true while the database is known to be down, to stop log spam
_ops_lock = threading.Lock()
_ops_connection = None
_ops_failed = False
_last_successful_write: str | None = None
_last_error: dict[str, str] | None = None


def database_write_status() -> dict:
    """Return process-local database write diagnostics without credentials."""
    return {
        "last_successful_write": _last_successful_write,
        "last_error": dict(_last_error) if _last_error else None,
    }


def _report_write_failure(operation: str, exc: Exception) -> None:
    global _last_error
    safe_error = f"{type(exc).__name__}: {str(exc)[:300]}"
    _last_error = {"operation": operation, "error": safe_error, "at": datetime.now(timezone.utc).isoformat()}
    try:
        from ops_observability import emit_event
        emit_event(
            "DATABASE_WRITE_FAILED", severity="ERROR", component="database",
            message="A best-effort database write failed; CSV/trading flow continues.",
            reason=type(exc).__name__,
            details={"operation": operation, "error": safe_error},
        )
    except Exception:
        # Observability must not become a dependency of persistence or trading.
        pass


def _dsn() -> str:
    """Connection string from the environment; never hardcoded."""
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    host = os.environ.get("POSTGRES_HOST", "postgres").strip()
    port = os.environ.get("POSTGRES_PORT", "5432").strip()
    name = os.environ.get("POSTGRES_DB", "watchtower").strip()
    user = os.environ.get("POSTGRES_USER", "").strip()
    password = os.environ.get("POSTGRES_PASSWORD", "").strip()
    if not user or not password:
        return ""
    return f"host={host} port={port} dbname={name} user={user} password={password}"


def _connect():
    """A live connection, reconnecting after an outage. None when unavailable."""
    global _connection, _failed

    if not _ENABLED:
        return None
    if _connection is not None and not getattr(_connection, "closed", 1):
        return _connection

    dsn = _dsn()
    if not dsn:
        if not _failed:
            logger.warning("database not configured; continuing with CSV only")
            _failed = True
        return None

    try:
        import psycopg

        _connection = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
        if _failed:
            logger.info("database connection restored")
        _failed = False
        return _connection
    except Exception as exc:  # pragma: no cover - depends on the environment
        if not _failed:
            logger.warning("database unavailable (%s); continuing with CSV only", type(exc).__name__)
            _failed = True
        _connection = None
        return None


def _connect_ops():
    """Use a separate connection so diagnostics never contend with trade writes."""
    global _ops_connection, _ops_failed
    if not _ENABLED:
        return None
    if _ops_connection is not None and not getattr(_ops_connection, "closed", 1):
        return _ops_connection
    dsn = _dsn()
    if not dsn:
        return None
    try:
        import psycopg
        _ops_connection = psycopg.connect(dsn, autocommit=True, connect_timeout=2)
        _ops_failed = False
        return _ops_connection
    except Exception:
        _ops_failed = True
        _ops_connection = None
        return None


def _execute(sql: str, params: tuple, *, operation: str = "database_write", emit_failure: bool = True, passive: bool = False) -> None:
    """Run one statement, swallowing any failure. Trading comes first."""
    global _connection, _failed, _last_successful_write
    if passive:
        global _ops_connection, _ops_failed
        with _ops_lock:
            connection = _connect_ops()
            if connection is None:
                if emit_failure and _ENABLED and _dsn():
                    _report_write_failure(operation, RuntimeError("database unavailable"))
                return
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                _last_successful_write = datetime.now(timezone.utc).isoformat()
            except Exception as exc:  # pragma: no cover - depends on environment
                _ops_failed = True
                try:
                    connection.close()
                except Exception:
                    pass
                _ops_connection = None
                if emit_failure:
                    _report_write_failure(operation, exc)
        return
    with _lock:
        connection = _connect()
        if connection is None:
            # CSV-only instances intentionally have no database; that is not
            # an operational failure and must not fill their event stream.
            if emit_failure and _ENABLED and _dsn():
                _report_write_failure(operation, RuntimeError("database unavailable"))
            return
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
            _last_successful_write = datetime.now(timezone.utc).isoformat()
        except Exception as exc:  # pragma: no cover - depends on the environment
            if not _failed:
                logger.warning("database write failed (%s); continuing with CSV only", type(exc).__name__)
                _failed = True
            try:
                connection.close()
            except Exception:
                pass
            _connection = None
            if emit_failure:
                _report_write_failure(operation, exc)


def _number(value: Any):
    """Floats arrive from CSV-shaped values, so blanks become NULL."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# ── signals (confluence_history) ───────────────────────────────────────────


def record_signal(csv_row: int, row: dict) -> None:
    """A signal the confluence engine found, as already written to CSV."""
    _execute(
        """
        INSERT INTO signals (
            csv_row, signal_time, symbol, timeframe, direction, score,
            h1_bias, liquidity_sweep, m15_structure, m5_fvg, m5_order_block,
            ob_lower, ob_upper, setup_id, status, rejection_reason,
            entry, stop_loss, take_profit, contract_id, outcome, profit,
            entry_time, exit_time, quality_score, quality_category, quality_components,
            displacement_assessment, premium_discount_state, tp_source,
            structural_target, potential_rr, setup_invalidation_reason
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (csv_row) DO UPDATE SET
            signal_time = EXCLUDED.signal_time,
            symbol      = EXCLUDED.symbol,
            timeframe   = EXCLUDED.timeframe,
            direction   = EXCLUDED.direction,
            status      = EXCLUDED.status,
            quality_score = EXCLUDED.quality_score,
            quality_category = EXCLUDED.quality_category,
            quality_components = EXCLUDED.quality_components,
            displacement_assessment = EXCLUDED.displacement_assessment,
            premium_discount_state = EXCLUDED.premium_discount_state,
            tp_source = EXCLUDED.tp_source,
            structural_target = EXCLUDED.structural_target,
            potential_rr = EXCLUDED.potential_rr,
            setup_invalidation_reason = EXCLUDED.setup_invalidation_reason,
            updated_at  = now()
        """,
        (
            csv_row, _text(row.get("signal_time")), _text(row.get("symbol")),
            _text(row.get("timeframe")), _text(row.get("direction")), _number(row.get("score")),
            _text(row.get("h1_bias")), _text(row.get("liquidity_sweep")), _text(row.get("m15_structure")),
            _text(row.get("m5_fvg")), _text(row.get("m5_order_block")),
            _number(row.get("ob_lower")), _number(row.get("ob_upper")),
            _text(row.get("setup_id")), _text(row.get("status")), _text(row.get("rejection_reason")),
            _number(row.get("entry")), _number(row.get("stop_loss")), _number(row.get("take_profit")),
            _text(row.get("contract_id")), _text(row.get("outcome")), _number(row.get("profit")),
            _text(row.get("entry_time")), _text(row.get("exit_time")),
            _number(row.get("quality_score")), _text(row.get("quality_category")),
            _text(row.get("quality_components")), _text(row.get("displacement_assessment")),
            _text(row.get("premium_discount_state")), _text(row.get("tp_source")),
            _number(row.get("structural_target")), _number(row.get("potential_rr")),
            _text(row.get("setup_invalidation_reason")),
        ),
        operation="record_signal",
    )


def update_signal(csv_row: int, values: dict) -> None:
    """Lifecycle change on a signal already recorded."""
    allowed = {
        "status": _text, "rejection_reason": _text, "entry": _number,
        "stop_loss": _number, "take_profit": _number, "contract_id": _text,
        "outcome": _text, "profit": _number, "entry_time": _text, "exit_time": _text,
        "timeframe": _text,
        "quality_score": _number, "quality_category": _text,
        "quality_components": _text, "displacement_assessment": _text,
        "premium_discount_state": _text, "tp_source": _text,
        "structural_target": _number, "potential_rr": _number,
        "setup_invalidation_reason": _text,
    }
    fields = {key: cast(values[key]) for key, cast in allowed.items() if key in values}
    if not fields:
        return
    assignments = ", ".join(f"{key} = %s" for key in fields)
    _execute(
        f"UPDATE signals SET {assignments}, updated_at = now() WHERE csv_row = %s",
        (*fields.values(), csv_row),
        operation="update_signal",
    )


# ── account (broker figures) ───────────────────────────────────────────────


def record_balance(
    balance: float,
    currency: str | None,
    equity: float | None = None,
    open_contracts: int | None = None,
    open_profit: float | None = None,
    account_id: str | None = None,
) -> bool:
    """Store one snapshot of the broker's own figures.

    Returns whether it was written, so the caller can report the outcome
    rather than assume it.
    """
    before = _failed
    _execute(
        """
        INSERT INTO balance_snapshots
            (balance, currency, equity, open_contracts, open_profit, account_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (balance, _text(currency), equity, open_contracts, open_profit, _text(account_id)),
        operation="record_balance",
    )
    return not _failed and (before or True)


# ── trades (trade_history) ─────────────────────────────────────────────────


def record_trade_row(csv_row: int, row: dict) -> None:
    """An order Watch Tower submitted, as already written to CSV."""
    _execute(
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
            contract_created_at, contract_updated_at, final_pl,
            quality_score, quality_category, quality_components,
            displacement_assessment, premium_discount_state, tp_source,
            structural_target, potential_rr, setup_invalidation_reason
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (csv_row) DO UPDATE SET
            status     = EXCLUDED.status,
            updated_at = now()
        """,
        (
            csv_row, _text(row.get("signal_time")), _text(row.get("symbol")),
            _text(row.get("direction")), _number(row.get("score")), _number(row.get("entry")),
            _number(row.get("stop_loss_price")), _number(row.get("take_profit_price")),
            _number(row.get("stop_distance_pct")),
            _number(row.get("stake")), _number(row.get("multiplier")),
            _number(row.get("risk_amount")), _number(row.get("reward_amount")),
            _text(row.get("contract_id")), _text(row.get("transaction_id")),
            _text(row.get("status")), _text(row.get("exit_time")),
            _number(row.get("profit")), _text(row.get("error")),
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
            _text(row.get("contract_created_at")),
            _text(row.get("contract_updated_at")),
            _number(row.get("final_pl")),
            _number(row.get("quality_score")), _text(row.get("quality_category")),
            _text(row.get("quality_components")), _text(row.get("displacement_assessment")),
            _text(row.get("premium_discount_state")), _text(row.get("tp_source")),
            _number(row.get("structural_target")), _number(row.get("potential_rr")),
            _text(row.get("setup_invalidation_reason")),
        ),
        operation="record_trade_row",
    )


def update_trade_row(csv_row: int, values: dict) -> None:
    """Lifecycle change on a trade already recorded, including its result."""
    allowed = {
        "status": _text, "exit_time": _text, "profit": _number, "error": _text,
        "contract_id": _text, "transaction_id": _text, "entry": _number,
        "stake": _number, "multiplier": _number, "risk_amount": _number,
        "reward_amount": _number,
        "entry_price": _number, "technical_sl_price": _number,
        "technical_tp_price": _number,
        "technical_stop_distance": _number,
        "technical_reward_distance": _number, "technical_rr": _number,
        "deriv_stop_loss_amount": _number,
        "deriv_take_profit_amount": _number,
        "broker_stop_loss_value": _number,
        "broker_take_profit_value": _number,
        "contract_created_at": _text, "contract_updated_at": _text,
        "final_pl": _number,
        "quality_score": _number, "quality_category": _text,
        "quality_components": _text, "displacement_assessment": _text,
        "premium_discount_state": _text, "tp_source": _text,
        "structural_target": _number, "potential_rr": _number,
        "setup_invalidation_reason": _text,
    }
    fields = {key: cast(values[key]) for key, cast in allowed.items() if key in values}
    if not fields:
        return
    assignments = ", ".join(f"{key} = %s" for key in fields)
    _execute(
        f"UPDATE trades SET {assignments}, updated_at = now() WHERE csv_row = %s",
        (*fields.values(), csv_row),
        operation="update_trade_row",
    )


def record_operational_event(event: dict) -> None:
    """Persist one structured event; failures stay isolated from its caller."""
    _execute(
        """
        WITH inserted AS (
            INSERT INTO operational_events (
                event_id, occurred_at, instance_id, severity, component, event_type,
                symbol, timeframe, signal_id, trade_id, contract_id, message, reason, details
            ) VALUES (
                %s::uuid, %s::timestamptz, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            ) ON CONFLICT (event_id) DO NOTHING RETURNING event_id
        )
        DELETE FROM operational_events
        WHERE occurred_at < now() - interval '30 days'
           OR event_id NOT IN (
                SELECT event_id FROM operational_events
                ORDER BY occurred_at DESC, event_id DESC LIMIT 20000
           )
        """,
        (
            event.get("event_id"), event.get("occurred_at"), event.get("instance_id"),
            event.get("severity", "INFO"), event.get("component", "trader"),
            event.get("event_type", "UNKNOWN"), event.get("symbol"), event.get("timeframe"),
            event.get("signal_id"), event.get("trade_id"), event.get("contract_id"),
            event.get("message", ""), event.get("reason"),
            json.dumps(event.get("details") or {}, separators=(",", ":")),
        ),
        operation="record_operational_event", emit_failure=False, passive=True,
    )


def record_runtime_status(component: str, state: dict) -> None:
    """Replace one component's latest passive runtime snapshot."""
    instance_id = os.environ.get("INSTANCE_ID", "unconfigured")
    _execute(
        """
        INSERT INTO runtime_status (instance_id, component, state, updated_at)
        VALUES (%s, %s, %s::jsonb, now())
        ON CONFLICT (instance_id, component) DO UPDATE SET
            state = EXCLUDED.state, updated_at = EXCLUDED.updated_at
        """,
        (instance_id, component, json.dumps(state, separators=(",", ":"))),
        operation="record_runtime_status", passive=True,
    )
