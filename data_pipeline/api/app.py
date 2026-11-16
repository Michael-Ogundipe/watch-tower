"""Watch Tower API: read-only views over the signals and trades tables.

This service never trades and never writes to the trading tables. It reads
what live_trader.py recorded, so the dashboard cannot affect the strategy.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from identity import configured_instance_id
from semantics import active_signals_from_runtime_rows, actual_trade_where, redact_account_id

INSTANCE_ID = configured_instance_id()
API_STARTED_MONOTONIC = time.monotonic()
API_STARTED_AT = datetime.now(timezone.utc)
API_REQUEST_COUNT = 0
API_ERROR_COUNT = 0
API_LAST_ERROR: dict[str, Any] | None = None
API_OPS_LOG = RotatingFileHandler("/tmp/watchtower-api-ops.jsonl", maxBytes=1_000_000, backupCount=2, encoding="utf-8", delay=True)
API_OPS_LOG.setFormatter(logging.Formatter("%(message)s"))

app = FastAPI(title="Watch Tower API", version="1.0", docs_url="/api/docs")

STATIC = Path(__file__).with_name("static")

# Filters the dashboard is allowed to apply, mapped to the column they test.
SIGNAL_FILTERS = {"symbol": "symbol", "timeframe": "timeframe", "direction": "direction", "status": "status"}
TRADE_FILTERS = {"symbol": "symbol", "direction": "direction", "result": "status"}


def dsn() -> str:
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'watchtower')} "
        f"user={os.environ.get('POSTGRES_USER', '')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
    )


def query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    global API_LAST_ERROR
    try:
        with psycopg.connect(dsn(), connect_timeout=2) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
    except Exception as exc:
        API_LAST_ERROR = {"at": datetime.now(timezone.utc).isoformat(), "operation": "api_query", "error": type(exc).__name__}
        _api_event("DATABASE_QUERY_FAILED", "ERROR", "database", "API database query failed.",
                   reason=type(exc).__name__, details={"operation": "api_query"})
        raise


def _api_event(event_type: str, severity: str, component: str, message: str, *, reason=None, details=None) -> None:
    event = {
        "event_id": str(uuid.uuid4()), "occurred_at": datetime.now(timezone.utc).isoformat(),
        "instance_id": INSTANCE_ID, "severity": severity, "component": component,
        "event_type": event_type, "symbol": None, "timeframe": None,
        "signal_id": None, "trade_id": None, "contract_id": None,
        "message": message, "reason": reason, "details": details or {},
    }
    try:
        API_OPS_LOG.emit(logging.LogRecord("watchtower.api", logging.ERROR, "", 0, json.dumps(event), (), None))
        os.chmod(API_OPS_LOG.baseFilename, 0o600)
    except Exception:
        logging.getLogger("watchtower.api").error("structured API event fallback failed")


@app.middleware("http")
async def track_api_requests(request, call_next):
    global API_REQUEST_COUNT, API_ERROR_COUNT
    API_REQUEST_COUNT += 1
    try:
        response = await call_next(request)
    except Exception as exc:
        API_ERROR_COUNT += 1
        _api_event("API_REQUEST_FAILED", "ERROR", "api", "API request raised an exception.",
                   reason=type(exc).__name__, details={"path": request.url.path})
        raise
    if response.status_code >= 500:
        API_ERROR_COUNT += 1
        _api_event("API_REQUEST_FAILED", "ERROR", "api", "API request returned a server error.",
                   reason=f"HTTP_{response.status_code}", details={"path": request.url.path})
    return response


def _where(filters: dict[str, str], values: dict[str, Any], start: str | None, end: str | None) -> tuple[str, list]:
    """Build a WHERE clause from the filters the caller actually supplied."""
    clauses: list[str] = []
    params: list[Any] = []
    for name, column in filters.items():
        value = values.get(name)
        if value:
            clauses.append(f"{column} = %s")
            params.append(value)
    # Dates compare against the ISO-8601 signal_time the pipeline writes.
    if start:
        clauses.append("signal_time >= %s")
        params.append(start)
    if end:
        clauses.append("signal_time <= %s")
        params.append(end)
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


def _safe_account_id(value: Any) -> str | None:
    return redact_account_id(value)


def _runtime_rows() -> list[dict[str, Any]]:
    return query(
        "SELECT component, state, updated_at FROM runtime_status "
        "WHERE instance_id = %s ORDER BY component",
        (INSTANCE_ID,),
    )


def _read_event_files() -> list[dict[str, Any]]:
    """Read only the bounded, dedicated fallback event files, never env or journals."""
    result: list[dict[str, Any]] = []
    roots = [Path(os.environ.get("WATCHTOWER_OPS_DIR", "/srv/ops-data"))]
    roots.append(Path("/tmp"))
    for root in roots:
        try:
            patterns = ("operational_events.*.jsonl*",) if root.name != "tmp" else ("watchtower-api-ops.jsonl*",)
            for pattern in patterns:
                for path in root.glob(pattern):
                    if not path.is_file() or path.stat().st_size > 1_100_000:
                        continue
                    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines()[-1500:]:
                        try:
                            event = json.loads(raw)
                            if event.get("instance_id") == INSTANCE_ID and event.get("event_id"):
                                result.append(event)
                        except (json.JSONDecodeError, AttributeError):
                            continue
        except OSError:
            pass
    return result


@app.get("/health")
def health() -> dict:
    """API, database, and application heartbeat status without host access."""
    db_started = time.perf_counter()
    database: dict[str, Any] = {
        "ok": False, "detail": "unreachable", "latency_ms": None,
        "last_successful_write": None, "last_error": None,
    }
    try:
        query("SELECT 1")
        database.update(ok=True, detail="connected", latency_ms=round((time.perf_counter() - db_started) * 1000, 2))
        runtimes = _runtime_rows()
        db_events = query(
            "SELECT occurred_at, details FROM operational_events "
            "WHERE instance_id = %s AND event_type = 'DATABASE_WRITE_FAILED' "
            "ORDER BY occurred_at DESC LIMIT 1",
            (INSTANCE_ID,),
        )
        if db_events:
            database["last_error"] = {
                "at": db_events[0]["occurred_at"],
                "operation": (db_events[0]["details"] or {}).get("operation"),
                "error": (db_events[0]["details"] or {}).get("error", "Database write failed"),
            }
    except Exception:
        runtimes = []
        if API_LAST_ERROR:
            database["last_error"] = dict(API_LAST_ERROR)

    if not database.get("last_error"):
        fallback_error = next((event for event in reversed(_read_event_files())
                               if event.get("event_type") in {"DATABASE_WRITE_FAILED", "DATABASE_QUERY_FAILED"}), None)
        if fallback_error:
            database["last_error"] = {
                "at": fallback_error.get("occurred_at"),
                "operation": (fallback_error.get("details") or {}).get("operation"),
                "error": (fallback_error.get("details") or {}).get("error") or fallback_error.get("reason"),
            }

    try:
        writes = query(
            "SELECT max(t) AS last_write FROM ("
            "SELECT max(updated_at) AS t FROM trades UNION ALL "
            "SELECT max(updated_at) AS t FROM signals UNION ALL "
            "SELECT max(taken_at) AS t FROM balance_snapshots UNION ALL "
            "SELECT max(occurred_at) AS t FROM operational_events"
            ") writes"
        )
        database["last_successful_write"] = writes[0]["last_write"].isoformat() if writes and writes[0]["last_write"] else None
    except Exception:
        pass
    if API_LAST_ERROR and not database.get("last_error"):
        database["last_error"] = dict(API_LAST_ERROR)

    now = datetime.now(timezone.utc)
    runtimes_by_component = {row["component"]: row for row in runtimes}
    traders = [row for key, row in runtimes_by_component.items() if key.startswith("trader:")]

    def fresh(row: dict | None, seconds: int = 75) -> bool:
        if not row:
            return False
        age = (now - row["updated_at"]).total_seconds()
        return age <= seconds and bool((row.get("state") or {}).get("running", True))

    active_traders = [row for row in traders if fresh(row)]
    trader_states = [(row, row.get("state") or {}) for row in active_traders]
    latest_state = max(trader_states, key=lambda pair: pair[0]["updated_at"])[1] if trader_states else {}
    last_heartbeat = max((state.get("last_heartbeat_at") for _, state in trader_states if state.get("last_heartbeat_at")), default=None)
    last_candle = max((state.get("last_candle_at") for _, state in trader_states if state.get("last_candle_at")), default=None)
    market_connected = bool(active_traders) and all(state.get("market_data_connected") for _, state in trader_states)

    balance_row = runtimes_by_component.get("balance")
    balance_state = balance_row.get("state") or {} if balance_row else {}
    balance_fresh = fresh(balance_row, max(900, int(os.environ.get("WATCHTOWER_BALANCE_INTERVAL", "300")) * 3))
    api_uptime = int(time.monotonic() - API_STARTED_MONOTONIC)
    database_ok = bool(database["ok"])
    latest_confluence = None
    if database_ok:
        try:
            confluence_row = query("SELECT max(signal_time) AS latest FROM signals")
            latest_confluence = confluence_row[0]["latest"] if confluence_row else None
        except Exception:
            pass
    container_rows = []
    if INSTANCE_ID == "vps":
        container_rows = [
            {"name": "watchtower", "status": "running" if active_traders else "stale_or_stopped", "uptime_seconds": latest_state.get("process_uptime_seconds"), "restarts": None, "health": "application heartbeat", "last_error": latest_state.get("last_error")},
            {"name": "watchtower-balance", "status": "running" if balance_fresh else "stale_or_stopped", "uptime_seconds": balance_state.get("process_uptime_seconds"), "restarts": None, "health": "application heartbeat", "last_error": balance_state.get("last_error")},
            {"name": "watchtower-api", "status": "running", "uptime_seconds": api_uptime, "restarts": None, "health": "serving health request", "last_error": API_LAST_ERROR},
            {"name": "watchtower-db", "status": "healthy" if database_ok else "unreachable", "uptime_seconds": None, "restarts": None, "health": "database query", "last_error": database.get("last_error")},
        ]

    return {
        "status": "ok" if database_ok else "degraded",
        "instance_id": INSTANCE_ID,
        "database": database,
        "trader": {
            "status": "running" if active_traders else "stale_or_stopped",
            "uptime_seconds": latest_state.get("process_uptime_seconds"),
            "last_heartbeat": last_heartbeat,
            "last_processed_candle": last_candle,
        },
        "market_data": {
            "status": "connected" if market_connected else "disconnected_or_stale",
            "last_tick": max((state.get("last_tick_at") for _, state in trader_states if state.get("last_tick_at")), default=None),
            "last_candle": last_candle,
            "reconnect_count": sum(int(state.get("reconnect_count") or 0) for _, state in trader_states),
            "last_disconnect": max((state.get("last_disconnect_at") for _, state in trader_states if state.get("last_disconnect_at")), default=None),
            "last_reconnect": max((state.get("last_reconnect_at") for _, state in trader_states if state.get("last_reconnect_at")), default=None),
        },
        "deriv": {
            "status": "authenticated" if latest_state.get("deriv_authenticated") else "not_authenticated_or_stale",
            "account_id": latest_state.get("account_id_redacted"),
            "account_type": latest_state.get("account_type"),
            "balance": balance_state.get("balance", latest_state.get("balance")),
            "currency": balance_state.get("currency", latest_state.get("currency")),
            "open_contract_count": balance_state.get("open_contract_count", sum(int(state.get("open_contract_count") or 0) for _, state in trader_states)),
            "last_update": balance_state.get("last_successful_poll") or max((state.get("last_tick_at") for _, state in trader_states if state.get("last_tick_at")), default=None),
        },
        "balance_worker": {
            "status": "running" if balance_fresh else "stale_or_stopped",
            "last_successful_poll": balance_state.get("last_successful_poll"),
            "last_error": balance_state.get("last_error"),
            "poll_age_seconds": int((now - balance_row["updated_at"]).total_seconds()) if balance_row else None,
        },
        "api": {"status": "healthy", "started_at": API_STARTED_AT.isoformat(), "uptime_seconds": api_uptime,
                "requests": API_REQUEST_COUNT, "errors": API_ERROR_COUNT,
                "last_error": API_LAST_ERROR},
        "containers": container_rows,
        "last_confluence_time": latest_confluence,
        "last_signal_time": latest_confluence,  # compatibility alias; historical only
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/signals")
def list_signals(
    symbol: str | None = None,
    timeframe: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    start: str | None = Query(None, description="ISO date/time lower bound"),
    end: str | None = Query(None, description="ISO date/time upper bound"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    """Return only currently pending, actionable in-memory signals."""
    items = active_signals_from_runtime_rows(_runtime_rows())
    filtered = []
    for item in items:
        if symbol and item.get("symbol") != symbol: continue
        if timeframe and str(item.get("timeframe")) != str(timeframe): continue
        if direction and item.get("direction") != direction: continue
        if status and status.upper() not in {"PENDING", "ACTIVE"}: continue
        if start and str(item.get("signal_time")) < start: continue
        if end and str(item.get("signal_time")) > end: continue
        filtered.append(item)
    total = len(filtered)
    return {"total": total, "limit": limit, "offset": offset, "items": filtered[offset:offset + limit]}


@app.get("/api/confluences")
def list_confluences(
    symbol: str | None = None,
    timeframe: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    start: str | None = Query(None, description="ISO date/time lower bound"),
    end: str | None = Query(None, description="ISO date/time upper bound"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    """Return persisted historical confluence discoveries."""
    values = {"symbol": symbol, "timeframe": timeframe, "direction": direction, "status": status}
    where, params = _where(SIGNAL_FILTERS, values, start, end)
    total = query(f"SELECT count(*) AS n FROM signals{where}", tuple(params))[0]["n"]
    rows = query(
        f"SELECT * FROM signals{where} ORDER BY signal_time DESC, id DESC LIMIT %s OFFSET %s",
        (*params, limit, offset),
    )
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


@app.get("/api/confluences/{signal_id}")
def get_confluence(signal_id: int) -> dict:
    rows = query("SELECT * FROM signals WHERE id = %s", (signal_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Confluence not found")
    return rows[0]


@app.get("/api/signals/{signal_id}")
def get_signal(signal_id: str) -> dict:
    """Look up only a currently active setup; history is under /api/confluences."""
    for item in list_signals(limit=500, offset=0)["items"]:
        if str(item.get("signal_id")) == signal_id:
            return item
    raise HTTPException(status_code=404, detail="Active signal not found")


@app.get("/api/trades")
def list_trades(
    symbol: str | None = None,
    direction: str | None = None,
    result: str | None = Query(None, description="WIN, LOSS, BREAKEVEN or a pending status"),
    start: str | None = None,
    end: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    values = {"symbol": symbol, "direction": direction, "result": result}
    where, params = _where(TRADE_FILTERS, values, start, end)
    actual_where = actual_trade_where(where)
    total = query(f"SELECT count(*) AS n FROM trades{actual_where}", tuple(params))[0]["n"]
    rows = query(
        f"SELECT * FROM trades_with_r{actual_where} ORDER BY signal_time DESC, id DESC LIMIT %s OFFSET %s",
        (*params, limit, offset),
    )
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


@app.get("/api/trades/{trade_id}")
def get_trade(trade_id: int) -> dict:
    rows = query("SELECT * FROM trades_with_r WHERE id = %s AND contract_id IS NOT NULL", (trade_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Trade not found")
    return rows[0]


@app.get("/api/performance")
def performance(
    symbol: str | None = None,
    direction: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """Totals computed from the stored trades, never from a separate tally."""
    values = {"symbol": symbol, "direction": direction, "result": None}
    where, params = _where(TRADE_FILTERS, values, start, end)
    actual_where = actual_trade_where(where)
    rows = query(
        f"""
        SELECT
            count(*)                                            AS total_trades,
            count(*) FILTER (WHERE status = 'WIN')              AS wins,
            count(*) FILTER (WHERE status = 'LOSS')             AS losses,
            count(*) FILTER (WHERE status = 'BREAKEVEN')        AS breakeven,
            count(*) FILTER (WHERE status NOT IN ('WIN','LOSS','BREAKEVEN')
                             OR status IS NULL)                 AS open_or_pending,
            coalesce(sum(r_multiple), 0)                        AS net_r,
            avg(r_multiple)                                     AS average_r,
            coalesce(sum(profit), 0)                            AS net_profit
        FROM trades_with_r{actual_where}
        """,
        tuple(params),
    )
    stats = rows[0]
    settled = (stats["wins"] or 0) + (stats["losses"] or 0) + (stats["breakeven"] or 0)
    stats["settled_trades"] = settled
    stats["win_rate"] = round((stats["wins"] / settled) * 100, 2) if settled else None
    stats["net_r"] = round(float(stats["net_r"] or 0), 4)
    stats["average_r"] = round(float(stats["average_r"]), 4) if stats["average_r"] is not None else None
    stats["net_profit"] = round(float(stats["net_profit"] or 0), 2)
    return stats


@app.get("/api/account")
def account() -> dict:
    """The broker's own figures, never inferred from the trade journal.

    Balance and equity come from balance_snapshots, which the poller fills
    from Deriv. Starting balance is the first snapshot that was ever taken,
    so every derived figure is "since Watch Tower started watching", not
    since the account was opened. That distinction is reported alongside
    the numbers rather than left for the reader to assume.
    """
    latest = query("SELECT * FROM balance_snapshots ORDER BY taken_at DESC LIMIT 1")
    if not latest:
        return {
            "available": False,
            "reason": "No balance snapshot has been recorded yet.",
            "source": "Deriv account",
        }
    now = latest[0]

    first = query("SELECT * FROM balance_snapshots ORDER BY taken_at ASC LIMIT 1")[0]
    # An explicit starting balance wins, for an account that was already
    # running before snapshots began.
    configured = os.environ.get("WATCHTOWER_STARTING_BALANCE", "").strip()
    try:
        starting = float(configured) if configured else float(first["balance"])
        starting_source = "configured" if configured else "first snapshot"
    except ValueError:
        starting, starting_source = float(first["balance"]), "first snapshot"

    today_first = query(
        "SELECT balance FROM balance_snapshots"
        " WHERE taken_at >= date_trunc('day', now() AT TIME ZONE 'UTC')"
        " ORDER BY taken_at ASC LIMIT 1"
    )
    balance = float(now["balance"])
    today_open = float(today_first[0]["balance"]) if today_first else None

    total_pl = round(balance - starting, 2)
    return {
        "available": True,
        "source": "Deriv account",
        "account_id": _safe_account_id(now["account_id"]),
        "currency": now["currency"],
        "balance": round(balance, 2),
        "equity": round(float(now["equity"]), 2) if now["equity"] is not None else None,
        "equity_note": None if now["equity"] is not None
                       else "Open positions could not be valued at this reading.",
        "open_contracts": now["open_contracts"],
        "open_profit": round(float(now["open_profit"]), 2) if now["open_profit"] is not None else None,
        "starting_balance": round(starting, 2),
        "starting_balance_source": starting_source,
        "tracking_since": first["taken_at"],
        "today_profit": round(balance - today_open, 2) if today_open is not None else None,
        "total_profit": total_pl,
        "profit_percent": round((total_pl / starting) * 100, 2) if starting else None,
        "last_sync": now["taken_at"],
        "snapshots": query("SELECT count(*) AS n FROM balance_snapshots")[0]["n"],
    }


@app.get("/api/account/history")
def account_history(
    start: str | None = None,
    end: str | None = None,
    limit: int = Query(500, ge=1, le=5000),
) -> dict:
    """Balance and equity over time, oldest first, for charting."""
    clauses, params = [], []
    if start:
        clauses.append("taken_at >= %s")
        params.append(start)
    if end:
        clauses.append("taken_at <= %s")
        params.append(end)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    rows = query(
        f"SELECT taken_at, balance, equity, currency, open_contracts"
        f" FROM balance_snapshots{where} ORDER BY taken_at ASC LIMIT %s",
        (*params, limit),
    )
    return {"count": len(rows), "items": rows}


@app.get("/api/summary")
def summary() -> dict:
    """What the dashboard header needs in one call."""
    stats = performance()
    confluences = query("SELECT count(*) AS total, max(signal_time) AS latest FROM signals")[0]
    active_signals = list_signals(limit=500, offset=0)["total"]
    per_symbol = query(
        """
        SELECT symbol,
               count(*) AS trades,
               count(*) FILTER (WHERE status = 'WIN')  AS wins,
               count(*) FILTER (WHERE status = 'LOSS') AS losses,
               round(coalesce(sum(r_multiple), 0)::numeric, 4) AS net_r
        FROM trades_with_r
        WHERE symbol IS NOT NULL AND contract_id IS NOT NULL
        GROUP BY symbol
        ORDER BY symbol
        """
    )
    return {
        "performance": stats,
        "signals": {"active": active_signals},
        "confluences": confluences,
        "by_symbol": per_symbol,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/filters")
def filters() -> dict:
    """The values actually present, so the dashboard offers only real options."""
    return {
        "symbols": [r["symbol"] for r in query(
            "SELECT DISTINCT symbol FROM trades WHERE symbol IS NOT NULL AND contract_id IS NOT NULL"
            " UNION SELECT DISTINCT symbol FROM signals WHERE symbol IS NOT NULL ORDER BY 1")],
        "timeframes": [r["timeframe"] for r in query(
            "SELECT DISTINCT timeframe FROM signals WHERE timeframe IS NOT NULL ORDER BY 1")],
        "directions": [r["direction"] for r in query(
            "SELECT DISTINCT direction FROM trades WHERE direction IS NOT NULL AND contract_id IS NOT NULL ORDER BY 1")],
        "results": [r["status"] for r in query(
            "SELECT DISTINCT status FROM trades WHERE status IS NOT NULL AND contract_id IS NOT NULL ORDER BY 1")],
    }


@app.get("/api/events")
def list_events(
    instance: str | None = None,
    severity: str | None = None,
    component: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
    event_type: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    """Recent structured events, with bounded JSONL fallback during DB outages."""
    requested_instance = instance or INSTANCE_ID
    if requested_instance not in {"local", "vps"}:
        raise HTTPException(status_code=400, detail="Invalid instance")
    conditions, values = ["instance_id = %s"], [requested_instance]
    for key, value in (("severity", severity), ("component", component), ("symbol", symbol), ("timeframe", timeframe), ("event_type", event_type)):
        if value:
            conditions.append(f"{key} = %s")
            values.append(value.upper() if key == "severity" else value)
    if start:
        conditions.append("occurred_at >= %s::timestamptz")
        values.append(start)
    if end:
        conditions.append("occurred_at <= %s::timestamptz")
        values.append(end)
    events: dict[str, dict[str, Any]] = {}
    try:
        rows = query(
            "SELECT event_id::text AS event_id, occurred_at, instance_id, severity, component, "
            "event_type, symbol, timeframe, signal_id, trade_id, contract_id, message, reason, details "
            f"FROM operational_events WHERE {' AND '.join(conditions)} "
            "ORDER BY occurred_at DESC LIMIT 2000",
            tuple(values),
        )
        for row in rows:
            row["occurred_at"] = row["occurred_at"].isoformat()
            events[row["event_id"]] = row
    except Exception:
        pass
    for event in _read_event_files():
        if event.get("instance_id") != requested_instance:
            continue
        if severity and event.get("severity") != severity.upper(): continue
        if component and event.get("component") != component: continue
        if symbol and event.get("symbol") != symbol: continue
        if timeframe and str(event.get("timeframe")) != str(timeframe): continue
        if event_type and event.get("event_type") != event_type: continue
        occurred = str(event.get("occurred_at", ""))
        if start and occurred < start: continue
        if end and occurred > end: continue
        events.setdefault(event["event_id"], event)
    items = sorted(events.values(), key=lambda event: str(event.get("occurred_at", "")), reverse=True)
    return {"total": len(items), "limit": limit, "offset": offset, "items": items[offset:offset + limit]}


@app.get("/api/event-filters")
def event_filters() -> dict:
    """Filter values available in the retained event window."""
    values = _read_event_files()
    try:
        values.extend(query(
            "SELECT event_id::text AS event_id, occurred_at, instance_id, severity, component, event_type, symbol, timeframe "
            "FROM operational_events WHERE instance_id = %s ORDER BY occurred_at DESC LIMIT 2000",
            (INSTANCE_ID,),
        ))
    except Exception:
        pass
    return {
        "instances": sorted({e.get("instance_id") for e in values if e.get("instance_id")}),
        "severities": sorted({e.get("severity") for e in values if e.get("severity")}),
        "components": sorted({e.get("component") for e in values if e.get("component")}),
        "symbols": sorted({e.get("symbol") for e in values if e.get("symbol")}),
        "timeframes": sorted({str(e.get("timeframe")) for e in values if e.get("timeframe")}),
        "event_types": sorted({e.get("event_type") for e in values if e.get("event_type")}),
    }


@app.get("/")
def dashboard() -> FileResponse:
    return FileResponse(STATIC / "index.html")


if STATIC.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC), name="static")
