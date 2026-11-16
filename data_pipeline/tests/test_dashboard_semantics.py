from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from api.semantics import active_signals_from_runtime_rows, actual_trade_where, redact_account_id


def import_api_app_without_optional_web_dependencies():
    """Load route functions with tiny framework stubs when API extras are absent."""
    api_dir = str(Path(__file__).resolve().parents[1] / "api")
    if api_dir not in sys.path:
        sys.path.insert(0, api_dir)
    saved = {}

    if "fastapi" not in sys.modules:
        fake = types.ModuleType("fastapi")
        class FastAPI:
            def __init__(self, **kwargs): pass
            def get(self, *args, **kwargs): return lambda fn: fn
            def middleware(self, *args, **kwargs): return lambda fn: fn
            def mount(self, *args, **kwargs): pass
        class HTTPException(Exception):
            def __init__(self, status_code, detail): self.status_code, self.detail = status_code, detail
        fake.FastAPI, fake.HTTPException = FastAPI, HTTPException
        fake.Query = lambda default=None, **kwargs: default
        saved["fastapi"] = None
        sys.modules["fastapi"] = fake
    if "fastapi.responses" not in sys.modules:
        mod = types.ModuleType("fastapi.responses")
        mod.FileResponse = type("FileResponse", (), {})
        saved["fastapi.responses"] = None
        sys.modules["fastapi.responses"] = mod
    if "fastapi.staticfiles" not in sys.modules:
        mod = types.ModuleType("fastapi.staticfiles")
        mod.StaticFiles = type("StaticFiles", (), {"__init__": lambda self, **kwargs: None})
        saved["fastapi.staticfiles"] = None
        sys.modules["fastapi.staticfiles"] = mod
    if "psycopg" not in sys.modules:
        mod = types.ModuleType("psycopg")
        mod.connect = lambda *args, **kwargs: None
        saved["psycopg"] = None
        sys.modules["psycopg"] = mod
    if "psycopg.rows" not in sys.modules:
        mod = types.ModuleType("psycopg.rows")
        mod.dict_row = object()
        saved["psycopg.rows"] = None
        sys.modules["psycopg.rows"] = mod
    try:
        with patch.dict(os.environ, {"INSTANCE_ID": "vps"}):
            import api.app as module
        return module
    finally:
        for name in saved:
            sys.modules.pop(name, None)


api_app = import_api_app_without_optional_web_dependencies()


class DashboardSemanticsTests(unittest.TestCase):
    def test_historical_found_confluence_is_not_an_active_signal(self):
        now = datetime.now(timezone.utc)
        # A historical FOUND row is not a runtime pending_signal snapshot.
        rows = [{"component": "trader:JD75", "updated_at": now,
                 "state": {"running": True, "symbol": "JD75", "pending_signal": None}}]
        self.assertEqual(active_signals_from_runtime_rows(rows, now=now), [])

    def test_fresh_pending_runtime_signal_is_active(self):
        now = datetime.now(timezone.utc)
        pending = {"signal_id": "setup-1", "direction": "BEARISH", "score": 7,
                   "timeframe": 300, "ob_lower": 10.0, "ob_upper": 12.0}
        rows = [{"component": "trader:JD25", "updated_at": now,
                 "state": {"running": True, "symbol": "JD25", "pending_signal": pending}}]
        active = active_signals_from_runtime_rows(rows, now=now)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["status"], "PENDING")
        self.assertEqual(active[0]["signal_id"], "setup-1")

    def test_stale_or_stopped_pending_snapshot_is_not_active(self):
        now = datetime.now(timezone.utc)
        pending = {"signal_id": "old", "direction": "BULLISH"}
        rows = [
            {"component": "trader:R_75", "updated_at": now - timedelta(seconds=90),
             "state": {"running": True, "pending_signal": pending}},
            {"component": "trader:JD75", "updated_at": now,
             "state": {"running": False, "pending_signal": pending}},
        ]
        self.assertEqual(active_signals_from_runtime_rows(rows, now=now), [])

    def test_trade_query_filter_excludes_rows_without_broker_contract(self):
        self.assertEqual(actual_trade_where(""), " WHERE contract_id IS NOT NULL")
        self.assertEqual(actual_trade_where(" WHERE symbol = %s"), " WHERE contract_id IS NOT NULL AND symbol = %s")

    def test_confluence_history_remains_available_separately(self):
        recorded = [{"id": 12, "status": "FOUND", "symbol": "JD75"}]
        calls = []
        def fake_query(sql, params=()):
            calls.append(sql)
            if "count(*)" in sql: return [{"n": len(recorded)}]
            return recorded
        with patch.object(api_app, "query", side_effect=fake_query):
            response = api_app.list_confluences()
        self.assertEqual(response["items"], recorded)
        self.assertTrue(any("FROM signals" in sql for sql in calls))

    def test_active_signals_route_uses_runtime_pending_state_not_history_table(self):
        now = datetime.now(timezone.utc)
        pending = {"signal_id": "setup-2", "direction": "BULLISH", "score": 5}
        with patch.object(api_app, "_runtime_rows", return_value=[{
            "component": "trader:R_75", "updated_at": now,
            "state": {"running": True, "symbol": "R_75", "pending_signal": pending},
        }]):
            response = api_app.list_signals()
        self.assertEqual(response["total"], 1)
        self.assertEqual(response["items"][0]["status"], "PENDING")

    def test_trade_history_route_filters_to_actual_contract_records(self):
        seen = []
        def fake_query(sql, params=()):
            seen.append(sql)
            return [{"n": 0}] if "count(*)" in sql else []
        with patch.object(api_app, "query", side_effect=fake_query):
            self.assertEqual(api_app.list_trades()["total"], 0)
        self.assertTrue(all("contract_id IS NOT NULL" in sql for sql in seen))

    def test_health_endpoint_reports_instance_and_never_credentials(self):
        now = datetime.now(timezone.utc)
        def fake_query(sql, params=()):
            if sql == "SELECT 1": return []
            if "FROM runtime_status" in sql:
                return [{"component": "trader:JD75", "updated_at": now,
                         "state": {"running": True, "deriv_authenticated": True,
                                   "account_id_redacted": "…635", "account_type": "demo",
                                   "market_data_connected": True, "last_tick_at": now.isoformat(),
                                   "last_candle_at": now.isoformat(), "process_uptime_seconds": 4,
                                   "open_contract_count": 0, "pending_signal": None}},
                        {"component": "balance", "updated_at": now,
                         "state": {"running": True, "balance": 10000, "currency": "USD",
                                   "open_contract_count": 0, "last_successful_poll": now.isoformat()}}]
            if "DATABASE_WRITE_FAILED" in sql: return []
            if "max(signal_time)" in sql: return [{"latest": None}]
            if "last_write" in sql: return [{"last_write": None}]
            return []
        with patch.object(api_app, "query", side_effect=fake_query), patch.object(api_app, "_read_event_files", return_value=[]):
            result = api_app.health()
        encoded = json.dumps(result)
        self.assertEqual(result["instance_id"], "vps")
        self.assertEqual(result["deriv"]["account_id"], "…635")
        self.assertNotIn("DERIV_API_TOKEN", encoded)
        self.assertNotIn("POSTGRES_PASSWORD", encoded)
        self.assertNotIn("not-a-real-secret", encoded)

    def test_account_identifier_is_redacted(self):
        self.assertEqual(redact_account_id("DOT123456635"), "…635")
        self.assertIsNone(redact_account_id(None))

    def test_dashboard_has_empty_states_for_active_confluence_and_trade_tables(self):
        html = (Path(__file__).resolve().parents[1] / "api/static/index.html").read_text()
        self.assertIn("No active signals", html)
        self.assertIn("No confluence history matches these filters", html)
        self.assertIn("No trades match these filters", html)


if __name__ == "__main__":
    unittest.main()
