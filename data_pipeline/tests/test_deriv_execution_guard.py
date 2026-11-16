import unittest
from unittest.mock import AsyncMock

from deriv_trading_client import DerivTradingClient, ExecutionPriceMovedError
from models import Tick
from parser import parse_tick


class DerivExecutionGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_proposal_spot_movement_blocks_buy_request(self):
        client = DerivTradingClient("app", "token-not-used")
        client._request_multiplier_proposal = AsyncMock(return_value={
            "proposal": {"id": "proposal-id", "spot": 101.5, "ask_price": 1.0}
        })
        client._send = AsyncMock()
        with self.assertRaises(ExecutionPriceMovedError):
            await client.buy_multiplier(
                symbol="R_75", direction="up", stake=1, multiplier=100,
                stop_loss=5, take_profit=10, reference_quote=100,
                max_quote_deviation=1,
            )
        client._send.assert_not_awaited()

    async def test_proposal_inside_tolerance_allows_buy_request(self):
        client = DerivTradingClient("app", "token-not-used")
        client._request_multiplier_proposal = AsyncMock(return_value={
            "proposal": {"id": "proposal-id", "spot": 100.5, "ask_price": 1.0}
        })
        client._send = AsyncMock(return_value={"buy": {"contract_id": 1}})
        # Stop/target update is the next _send after the buy response.
        client._send.side_effect = [
            {"buy": {"contract_id": 1, "transaction_id": 2}},
            {"contract_update": {}},
        ]
        await client.buy_multiplier(
            symbol="R_75", direction="up", stake=1, multiplier=100,
            stop_loss=5, take_profit=10, reference_quote=100,
            max_quote_deviation=1,
        )
        self.assertEqual(client._send.await_count, 2)


class DerivTickParsingTests(unittest.TestCase):
    def test_tick_bid_ask_available_and_pip_size_may_be_missing(self):
        tick = parse_tick({"tick": {
            "symbol": "R_75", "quote": 10.25, "bid": 10.24, "ask": 10.26,
            "epoch": 1790870400,
        }})
        self.assertEqual(tick.bid, 10.24)
        self.assertEqual(tick.ask, 10.26)
        self.assertIsNone(tick.pip_size)


if __name__ == "__main__":
    unittest.main()
