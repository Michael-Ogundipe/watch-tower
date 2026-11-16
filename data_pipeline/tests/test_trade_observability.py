import asyncio
import csv
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import db_sink
import live_trader
import trade_history
from models import BreakDirection, OrderBlock, OrderBlockType, Timeframe
from deriv_trading_client import DerivTradingClient
from trade_observability import NOT_AVAILABLE


class FakeTradingClient:
    def __init__(self, contract_update=None):
        self.contract_update = contract_update or {}
        self.buy_args = None

    async def buy_multiplier(self, **kwargs):
        self.buy_args = kwargs
        return {
            "buy": {
                "contract_id": 123456,
                "transaction_id": 654321,
                "purchase_time": 1790862334,
            },
            "contract_update": self.contract_update,
        }

    async def subscribe_contract(self, _contract_id):
        yield {"is_sold": True, "profit": -5.0, "sell_time": 1790862390}


class TradeObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.history_path = Path(self.temp_dir.name) / "trade_history.csv"
        self.patches = [
            patch.object(trade_history, "HISTORY_FILE", self.history_path),
            patch.object(db_sink, "record_trade_row"),
            patch.object(db_sink, "update_trade_row"),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addCleanup(self.temp_dir.cleanup)

    @staticmethod
    def broker_update(include_values=True):
        stop = {"order_amount": -5, "order_date": 1790862335}
        target = {"order_amount": 10, "order_date": 1790862335}
        if include_values:
            stop["value"] = "102.35"
            target["value"] = "116.2"
        return {"stop_loss": stop, "take_profit": target}

    def run_pending_entry(self, client):
        trader = live_trader.LiveTrader(
            "R_75", client, multiplier=100, risk_amount=5.0,
            min_stake=1.0, max_stake=100.0,
        )
        trader.pending_signal = {
            "direction": BreakDirection.BULLISH,
            "order_block": OrderBlock(
                timeframe=Timeframe.M5,
                block_type=OrderBlockType.BULLISH,
                candle=None,
                upper=110.0,
                lower=100.0,
                bos=None,
            ),
            "score": 7,
            "ticks_waited": 0,
        }
        return trader

    @staticmethod
    def entry_tick():
        from datetime import datetime, timezone
        return SimpleNamespace(quote=105.0, bid=104.9, ask=105.1, pip_size=2,
                               timestamp=datetime.now(timezone.utc))

    def read_rows(self):
        with self.history_path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def test_strategy_prices_and_monetary_inputs_are_unchanged_and_broker_values_are_separate(self):
        client = FakeTradingClient(self.broker_update())
        trader = self.run_pending_entry(client)
        output = io.StringIO()
        with patch("live_trader.asyncio.create_task", side_effect=lambda coro: coro.close()), redirect_stdout(output):
            asyncio.run(trader._check_pending_entry(self.entry_tick()))

        self.assertEqual(client.buy_args["stop_loss"], 5.0)
        self.assertEqual(client.buy_args["take_profit"], 10.0)
        self.assertEqual(client.buy_args["stake"], 1.05)
        self.assertEqual(client.buy_args["multiplier"], 100)

        row = self.read_rows()[0]
        self.assertEqual(float(row["technical_sl_price"]), 100.0)
        self.assertEqual(float(row["technical_tp_price"]), 115.0)
        self.assertEqual(float(row["technical_stop_distance"]), 5.0)
        self.assertEqual(float(row["technical_reward_distance"]), 10.0)
        self.assertEqual(float(row["technical_rr"]), 2.0)
        self.assertEqual(float(row["deriv_stop_loss_amount"]), 5.0)
        self.assertEqual(float(row["deriv_take_profit_amount"]), 10.0)
        self.assertEqual(float(row["broker_stop_loss_value"]), 102.35)
        self.assertEqual(float(row["broker_take_profit_value"]), 116.2)
        self.assertEqual(row["contract_created_at"], "2026-10-01T13:45:34+00:00")
        self.assertEqual(row["contract_updated_at"], "2026-10-01T13:45:35+00:00")
        self.assertEqual(row["contract_id"], "123456")
        self.assertEqual(row["transaction_id"], "654321")

        log = output.getvalue()
        self.assertIn("technical_sl_price=100.0", log)
        self.assertIn("broker_stop_loss_value=102.35", log)
        self.assertIn("deriv_stop_loss_amount=5.0", log)
        self.assertNotIn("broker SL=", log)
        self.assertNotIn("broker TP=", log)

    def test_missing_broker_values_are_explicit_and_not_invented(self):
        client = FakeTradingClient(self.broker_update(include_values=False))
        trader = self.run_pending_entry(client)
        output = io.StringIO()
        with patch("live_trader.asyncio.create_task", side_effect=lambda coro: coro.close()), redirect_stdout(output):
            asyncio.run(trader._check_pending_entry(self.entry_tick()))

        row = self.read_rows()[0]
        self.assertEqual(row["broker_stop_loss_value"], NOT_AVAILABLE)
        self.assertEqual(row["broker_take_profit_value"], NOT_AVAILABLE)
        self.assertIn("broker_stop_loss_value=NOT_AVAILABLE", output.getvalue())
        self.assertIn("broker_take_profit_value=NOT_AVAILABLE", output.getvalue())

    def test_deriv_client_log_labels_response_values_and_amounts_separately(self):
        client = DerivTradingClient("test-app", "unused-test-token")
        client._request_multiplier_proposal = AsyncMock(
            return_value={"proposal": {"id": "proposal-id"}}
        )
        client._send = AsyncMock(side_effect=[
            {"buy": {"contract_id": 123456, "transaction_id": 654321}},
            {"contract_update": {
                "stop_loss": {"order_amount": -5, "value": "102.35"},
                "take_profit": {"order_amount": 10, "value": "116.2"},
            }},
        ])
        output = io.StringIO()
        with redirect_stdout(output):
            asyncio.run(client.buy_multiplier(
                symbol="R_75", direction="up", stake=1.05, multiplier=100,
                stop_loss=5.0, take_profit=10.0,
            ))

        log = output.getvalue()
        self.assertIn("deriv_stop_loss_amount=5.0", log)
        self.assertIn("broker_stop_loss_value=102.35", log)
        self.assertIn("deriv_take_profit_amount=10.0", log)
        self.assertIn("broker_take_profit_value=116.2", log)
        self.assertNotIn("SL=", log)
        self.assertNotIn("TP=", log)

    def test_final_profit_loss_is_persisted_in_explicit_column(self):
        client = FakeTradingClient(self.broker_update())
        trader = self.run_pending_entry(client)
        with patch("live_trader.asyncio.create_task", side_effect=lambda coro: coro.close()):
            asyncio.run(trader._check_pending_entry(self.entry_tick()))
        asyncio.run(trader._watch_open_contract(123456, 0))

        row = self.read_rows()[0]
        self.assertEqual(float(row["final_pl"]), -5.0)
        self.assertEqual(float(row["profit"]), -5.0)

    def test_old_trade_history_is_readable_and_new_columns_append_on_write(self):
        legacy_fields = trade_history.FIELDS[:18]
        old_row = {field: "" for field in legacy_fields}
        old_row.update({"signal_time": "2026-10-01T00:00:00+00:00", "symbol": "R_75", "entry": "100"})
        with self.history_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=legacy_fields)
            writer.writeheader()
            writer.writerow(old_row)

        # Existing callers can still read the original row with DictReader.
        with self.history_path.open(newline="", encoding="utf-8") as handle:
            before = next(csv.DictReader(handle))
        self.assertEqual(before["entry"], "100")

        trade_history.record_trade(symbol="JD75", direction="UP", entry=101.0, entry_price=101.0)
        rows = self.read_rows()
        self.assertEqual(rows[0]["entry"], "100")
        self.assertEqual(rows[0]["entry_price"], "")
        self.assertEqual(rows[1]["entry_price"], "101.0")
        self.assertIn("technical_sl_price", rows[0])
        self.assertIn("broker_stop_loss_value", rows[0])

    def test_restart_resumes_open_contract_and_reconciles_its_close(self):
        history_row = trade_history.record_trade(
            symbol="JD25",
            direction="DOWN",
            contract_id="123456",
            transaction_id="654321",
            status="OPEN",
        )
        client = FakeTradingClient(self.broker_update())
        trader = live_trader.LiveTrader("JD25", client)
        scheduled = []

        with patch(
            "live_trader.asyncio.create_task",
            side_effect=lambda coro: scheduled.append(coro),
        ), redirect_stdout(io.StringIO()):
            asyncio.run(trader._resume_open_contracts())

        self.assertEqual(trader.open_contract_id, "123456")
        self.assertEqual(len(scheduled), 1)
        self.assertIsNone(client.buy_args)

        asyncio.run(scheduled[0])

        row = self.read_rows()[history_row]
        self.assertEqual(row["status"], "LOSS")
        self.assertEqual(float(row["profit"]), -5.0)
        self.assertEqual(float(row["final_pl"]), -5.0)
        self.assertEqual(row["exit_time"], "2026-10-01T13:46:30+00:00")
        self.assertIsNone(trader.open_contract_id)


if __name__ == "__main__":
    unittest.main()
