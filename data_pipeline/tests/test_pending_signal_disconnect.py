from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from live_trader import LiveTrader


class PendingSignalDisconnectTests(unittest.TestCase):
    def make_trader(self, signal_time: str, disconnected_at: str | None) -> LiveTrader:
        trader = LiveTrader.__new__(LiveTrader)
        trader.symbol = "R_75"
        trader.pending_signal = {
            "signal_time": signal_time,
            "timeframe": "300",
            "setup_id": "setup-1",
        }
        trader.market_service = SimpleNamespace(
            client=SimpleNamespace(last_disconnect_at=disconnected_at)
        )
        trader._log = Mock()
        trader._publish_runtime_status = Mock()
        return trader

    def test_pending_signal_expires_on_first_tick_after_later_disconnect(self) -> None:
        trader = self.make_trader(
            "2026-10-03T16:05:00+00:00",
            "2026-10-03T17:39:47+00:00",
        )

        with patch("live_trader.emit_event") as emit:
            self.assertTrue(trader._expire_pending_after_market_data_gap())

        self.assertIsNone(trader.pending_signal)
        trader._publish_runtime_status.assert_called_once_with(force=True)
        emit.assert_called_once()
        self.assertEqual(emit.call_args.args[0], "SIGNAL_EXPIRED")
        self.assertEqual(emit.call_args.kwargs["reason"], "MARKET_DATA_DISCONNECTED")
        self.assertEqual(emit.call_args.kwargs["signal_id"], "setup-1")

    def test_disconnect_before_signal_does_not_expire_new_signal(self) -> None:
        trader = self.make_trader(
            "2026-10-03T17:40:00+00:00",
            "2026-10-03T17:39:47+00:00",
        )

        with patch("live_trader.emit_event") as emit:
            self.assertFalse(trader._expire_pending_after_market_data_gap())

        self.assertIsNotNone(trader.pending_signal)
        emit.assert_not_called()

    def test_no_pending_signal_is_a_no_op(self) -> None:
        trader = self.make_trader("2026-10-03T16:05:00+00:00", None)
        trader.pending_signal = None

        with patch("live_trader.emit_event") as emit:
            self.assertFalse(trader._expire_pending_after_market_data_gap())

        emit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
