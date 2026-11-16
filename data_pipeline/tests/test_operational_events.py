from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import db_sink
import deriv_client
import live_trader
import ops_observability
from models import BreakDirection, Timeframe
from order_block_detector import OrderBlock, OrderBlockType


class OperationalEventTests(unittest.TestCase):
    def test_structured_event_is_written_with_rejection_reason_and_secrets_redacted(self):
        with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {
            "DATA_DIR": root, "INSTANCE_ID": "vps", "INSTANCE_PROCESS": "trader"
        }, clear=False):
            ops_observability._HANDLERS.clear()
            ops_observability.emit_event(
                "SIGNAL_REJECTED", severity="WARN", component="trader", symbol="JD25",
                timeframe="300", signal_id="setup-test", message="Signal rejected",
                reason="OPEN_CONTRACT_ALREADY_EXISTS", details={"DERIV_API_TOKEN": "must-not-persist"},
            )
            self.assertTrue(ops_observability.flush())
            paths = list((Path(root) / "ops").glob("operational_events.*.jsonl"))
            self.assertEqual(len(paths), 1)
            stored = json.loads(paths[0].read_text().splitlines()[-1])
            self.assertEqual(stored["event_type"], "SIGNAL_REJECTED")
            self.assertEqual(stored["reason"], "OPEN_CONTRACT_ALREADY_EXISTS")
            self.assertEqual(stored["details"]["DERIV_API_TOKEN"], "***")
            self.assertNotIn("must-not-persist", paths[0].read_text())

    def test_database_failure_queues_an_observable_error_without_raising(self):
        class BrokenCursor:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, *args): raise RuntimeError("database write denied")
        class BrokenConnection:
            closed = 0
            def cursor(self): return BrokenCursor()
            def close(self): self.closed = 1
        captured = []
        with patch.object(db_sink, "_connect", return_value=BrokenConnection()), \
             patch("ops_observability.emit_event", side_effect=lambda *a, **k: captured.append((a, k))):
            db_sink._execute("INSERT", (), operation="record_signal")
        self.assertTrue(captured)
        args, kwargs = captured[0]
        self.assertEqual(args[0], "DATABASE_WRITE_FAILED")
        self.assertEqual(kwargs["details"]["operation"], "record_signal")

    def test_market_data_connect_and_reconnect_events_are_recorded(self):
        client = deriv_client.DerivClient()
        events = []
        with patch("deriv_client.websockets.connect", new=AsyncMock(side_effect=[object(), object()])), \
             patch("deriv_client.emit_event", side_effect=lambda *a, **k: events.append((a, k))), \
             redirect_stdout(io.StringIO()):
            asyncio.run(client.connect())
            asyncio.run(client.connect())
        self.assertEqual([event[0][0] for event in events], ["MARKET_DATA_CONNECTED", "MARKET_DATA_RECONNECTED"])
        self.assertEqual(client.reconnect_count, 1)

    def test_existing_stake_rejection_emits_actual_reason_without_order(self):
        class NeverBuyClient:
            async def buy_multiplier(self, **kwargs):
                raise AssertionError("not reached for an unachievable stake")
        trader = live_trader.LiveTrader("R_75", NeverBuyClient(), multiplier=100,
                                        risk_amount=5, min_stake=1, max_stake=1)
        trader.pending_signal = {
            "direction": BreakDirection.BULLISH,
            "order_block": OrderBlock(
                timeframe=Timeframe.M5, block_type=OrderBlockType.BULLISH,
                candle=None, upper=110.0, lower=100.0, bos=None,
            ),
            "score": 7, "ticks_waited": 0, "setup_id": "setup-no-size", "timeframe": 300,
        }
        events = []
        with patch("live_trader.emit_event", side_effect=lambda *a, **k: events.append((a, k))), \
             patch("live_trader.emit_runtime_status"), redirect_stdout(io.StringIO()):
            asyncio.run(trader._check_pending_entry(SimpleNamespace(
                quote=105.0, timestamp=datetime.now(timezone.utc), pip_size=2,
            )))
        rejected = next((kwargs for args, kwargs in events if args and args[0] == "SIGNAL_REJECTED"), None)
        self.assertIsNotNone(rejected)
        self.assertEqual(rejected["reason"], "STOP_DISTANCE_CANNOT_SUPPORT_RISK_WITHIN_STAKE_LIMITS")
        self.assertIsNone(trader.pending_signal)


if __name__ == "__main__":
    unittest.main()
