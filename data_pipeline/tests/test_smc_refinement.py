import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import db_sink
import live_trader
import trade_history
from bos_detector import BOSDetector
from confluence_engine import ConfluenceEngine
from liquidity_sweep_detector import LiquiditySweepDetector
from liquidity_detector import LiquidityDetector
from models import (
    BreakDirection,
    BreakOfStructure,
    BreakType,
    Candle,
    LiquidityPool,
    LiquidityType,
    OrderBlock,
    OrderBlockType,
    SwingPoint,
    SwingType,
    StructureBias,
    Timeframe,
)
from smc_refinement import (
    find_mitigated_htf_poi,
    find_post_mitigation_ltf_confirmation,
    select_structural_target,
)
from position_sizing import compute_stake
from trade_management import ProtectionState, update_protection


def bar(tf, epoch, open_, high, low, close):
    return Candle("R_75", tf, open_, high, low, close,
                  datetime.fromtimestamp(epoch, timezone.utc))


class SMCRefinementTests(unittest.TestCase):
    def htf_ltf_fixture(self):
        htf_bos_candle = bar(Timeframe.H1, 0, 100, 102, 98, 101)
        htf_bos = BreakOfStructure(Timeframe.H1, BreakDirection.BULLISH,
                                   BreakType.BOS, SwingPoint(htf_bos_candle, SwingType.HIGH), htf_bos_candle)
        htf_ob = OrderBlock(Timeframe.H1, OrderBlockType.BULLISH,
                            htf_bos_candle, 101, 99, htf_bos)
        touch = bar(Timeframe.M5, 3600, 100, 100.5, 98.8, 99.5)
        confirmation_candle = bar(Timeframe.M5, 3900, 99.5, 103, 99.4, 102)
        ltf_bos = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH,
                                   BreakType.BOS, SwingPoint(bar(Timeframe.M5, 3300, 100, 101, 99, 100), SwingType.HIGH),
                                   confirmation_candle)
        entry_ob = OrderBlock(Timeframe.M5, OrderBlockType.BULLISH,
                              bar(Timeframe.M5, 3300, 100, 100.5, 98.5, 99), 100.5, 98.5, ltf_bos)
        structures = {Timeframe.H1: SimpleNamespace(candles=[]), Timeframe.M5: SimpleNamespace(candles=[touch, confirmation_candle])}
        obs = {
            Timeframe.H1: SimpleNamespace(order_blocks=[htf_ob]),
            Timeframe.M5: SimpleNamespace(order_blocks=[entry_ob]),
        }
        fvgs = {Timeframe.H1: SimpleNamespace(fvgs=[])}
        return structures, obs, fvgs, touch, ltf_bos, entry_ob

    def test_htf_poi_mitigation_then_fresh_ltf_bos_provides_entry_ob_sl(self):
        structures, obs, fvgs, touch, ltf_bos, entry_ob = self.htf_ltf_fixture()
        current = datetime.fromtimestamp(5000, timezone.utc)
        poi = find_mitigated_htf_poi(BreakDirection.BULLISH, current, structures, obs, fvgs,
                                     structures[Timeframe.M5].candles)
        self.assertEqual(poi["mitigated_at"], datetime.fromtimestamp(3900, timezone.utc))
        result = find_post_mitigation_ltf_confirmation(
            BreakDirection.BULLISH, current, poi["mitigated_at"],
            SimpleNamespace(breaks=[ltf_bos]), obs[Timeframe.M5], timeframe=Timeframe.M5)
        self.assertIs(result[1], entry_ob)
        self.assertEqual(result[1].lower, 98.5)  # M5 structural invalidation, not H1 POI edge.

    def test_ltf_bos_before_poi_mitigation_does_not_confirm(self):
        structures, obs, fvgs, _, ltf_bos, _ = self.htf_ltf_fixture()
        result = find_post_mitigation_ltf_confirmation(
            BreakDirection.BULLISH, datetime.fromtimestamp(4000, timezone.utc),
            datetime.fromtimestamp(4300, timezone.utc),
            SimpleNamespace(breaks=[ltf_bos]), obs[Timeframe.M5], timeframe=Timeframe.M5)
        self.assertIsNone(result)

    def test_htf_poi_and_ltf_confirmation_are_limited_to_asof_time(self):
        structures, obs, fvgs, _, ltf_bos, _ = self.htf_ltf_fixture()
        before_confirmation = datetime.fromtimestamp(3890, timezone.utc)
        poi = find_mitigated_htf_poi(BreakDirection.BULLISH, before_confirmation, structures, obs, fvgs,
                                     structures[Timeframe.M5].candles)
        self.assertIsNone(poi)
        self.assertIsNone(find_post_mitigation_ltf_confirmation(
            BreakDirection.BULLISH, before_confirmation, datetime.fromtimestamp(3900, timezone.utc),
            SimpleNamespace(breaks=[ltf_bos]), obs[Timeframe.M5], timeframe=Timeframe.M5))

    def make_confluence_engine(self, ltf_break_epoch):
        from models import FairValueGap, FVGType, LiquiditySweep, FVGStatus
        htf_candle = bar(Timeframe.H1, 3600, 100, 102, 98, 101)
        htf_bos = BreakOfStructure(Timeframe.H1, BreakDirection.BULLISH, BreakType.BOS,
                                   SwingPoint(htf_candle, SwingType.HIGH), htf_candle)
        htf_ob = OrderBlock(Timeframe.H1, OrderBlockType.BULLISH, htf_candle, 101, 99, htf_bos)
        touch = bar(Timeframe.M5, 7200, 100, 100.5, 98.8, 99.5)
        ltf_confirm_candle = bar(Timeframe.M5, ltf_break_epoch, 99.5, 103, 99.4, 102)
        ltf_bos = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH, BreakType.BOS,
                                   SwingPoint(bar(Timeframe.M5, 6900, 100, 101, 99, 100), SwingType.HIGH),
                                   ltf_confirm_candle)
        entry_ob = OrderBlock(Timeframe.M5, OrderBlockType.BULLISH,
                              bar(Timeframe.M5, 6900, 100, 100.5, 98.5, 99), 100.5, 98.5, ltf_bos)
        sweep_candle = bar(Timeframe.M5, 9000, 100, 101, 98, 100)
        sweep = LiquiditySweep(Timeframe.M5, LiquidityType.SELL_SIDE,
                               LiquidityPool(Timeframe.M5, LiquidityType.SELL_SIDE, 99, []), sweep_candle)
        m15_candle = bar(Timeframe.M15, 10000, 100, 103, 99, 102)
        m15_break = BreakOfStructure(Timeframe.M15, BreakDirection.BULLISH, BreakType.BOS,
                                     SwingPoint(m15_candle, SwingType.HIGH), m15_candle)
        fvg_candle = bar(Timeframe.M5, 13000, 100, 104, 103, 103.5)
        fvg = FairValueGap(Timeframe.M5, FVGType.BULLISH, 103, 101, [fvg_candle] * 3, FVGStatus.ACTIVE)
        m5_bos_candle = bar(Timeframe.M5, 14000, 100, 104, 99, 103)
        m5_bos = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH, BreakType.BOS,
                                  SwingPoint(m5_bos_candle, SwingType.HIGH), m5_bos_candle)
        m5_ob = OrderBlock(Timeframe.M5, OrderBlockType.BULLISH,
                           bar(Timeframe.M5, 13500, 103, 104, 99, 100), 104, 99, m5_bos)
        structures = {tf: SimpleNamespace(candles=[], get_bias=lambda: StructureBias.BULLISH)
                      for tf in (Timeframe.H4, Timeframe.H1, Timeframe.M15, Timeframe.M5)}
        structures[Timeframe.M5].candles = [touch, ltf_confirm_candle]
        structures[Timeframe.H1] = SimpleNamespace(get_bias=lambda: StructureBias.BULLISH)
        fvg_detectors = {tf: SimpleNamespace(fvgs=[]) for tf in Timeframe}
        fvg_detectors[Timeframe.M5].fvgs = [fvg]
        order_blocks = {tf: SimpleNamespace(order_blocks=[]) for tf in Timeframe}
        order_blocks[Timeframe.H4] = SimpleNamespace(order_blocks=[])
        order_blocks[Timeframe.H1] = SimpleNamespace(order_blocks=[htf_ob])
        order_blocks[Timeframe.M5] = SimpleNamespace(order_blocks=[entry_ob, m5_ob])
        sweeps = {tf: SimpleNamespace(sweeps=[]) for tf in Timeframe}
        sweeps[Timeframe.M5].sweeps = [sweep]
        breaks = {tf: SimpleNamespace(breaks=[]) for tf in Timeframe}
        breaks[Timeframe.M15].breaks = [m15_break]
        breaks[Timeframe.M5].breaks = [ltf_bos]
        return ConfluenceEngine(structures, fvg_detectors, order_blocks, sweeps, breaks), entry_ob

    def test_full_confluence_keeps_existing_gates_and_uses_fresh_m5_entry_block_without_m1_state(self):
        engine, entry_ob = self.make_confluence_engine(7500)
        result = engine.analyze(datetime.fromtimestamp(20000, timezone.utc))
        self.assertIsNotNone(result)
        self.assertEqual(result["score"], 9)
        self.assertIn(result["quality"]["category"], {"A+", "A", "B", "C"})
        self.assertIn("displacement", result["quality"]["components"])
        self.assertIs(result["order_block"], entry_ob)
        self.assertIn("M5 after H1 POI mitigation", " ".join(result["reasons"]))

    def test_full_confluence_rejects_pre_mitigation_m5_break(self):
        engine, _ = self.make_confluence_engine(7200)
        self.assertIsNone(engine.analyze(datetime.fromtimestamp(20000, timezone.utc)))

    def test_structural_target_selects_nearest_opposing_level_with_buffer(self):
        swing = SwingPoint(bar(Timeframe.H1, 3600, 105, 110, 104, 108), SwingType.HIGH)
        structures = {Timeframe.H1: SimpleNamespace(swing_highs=[swing], swing_lows=[])}
        target = select_structural_target(
            105, BreakDirection.BULLISH, datetime.fromtimestamp(10000, timezone.utc),
            structures, {}, {}, 0.01)
        self.assertAlmostEqual(target, 109.99)

    def test_structural_target_ignores_future_swings(self):
        future = SwingPoint(bar(Timeframe.H1, 9000, 105, 110, 104, 108), SwingType.HIGH)
        structures = {Timeframe.H1: SimpleNamespace(swing_highs=[future], swing_lows=[])}
        self.assertIsNone(select_structural_target(
            105, BreakDirection.BULLISH, datetime.fromtimestamp(8000, timezone.utc),
            structures, {}, {}, 0.01))

    def test_structural_target_distance_drives_reward_amount_not_dollar_risk(self):
        sized = compute_stake(100, 95, 100, 5, reward_multiple=1.5,
                              min_stake=1, max_stake=100)
        self.assertEqual(sized.stop_loss_amount, 5.0)
        self.assertEqual(sized.take_profit_amount, 7.5)

    def test_sweep_requires_wick_through_and_close_back_inside_in_both_directions(self):
        detector = LiquiditySweepDetector()
        buy_pool = LiquidityPool(Timeframe.M5, LiquidityType.BUY_SIDE, 10, [])
        sell_pool = LiquidityPool(Timeframe.M5, LiquidityType.SELL_SIDE, 5, [])
        self.assertIsNotNone(detector.check(bar(Timeframe.M5, 0, 9, 11, 8, 9.5), [buy_pool]))
        self.assertIsNone(detector.check(bar(Timeframe.M5, 300, 9, 11, 8, 10.5), [buy_pool]))
        self.assertIsNotNone(detector.check(bar(Timeframe.M5, 600, 6, 7, 4, 5.5), [sell_pool]))
        self.assertIsNone(detector.check(bar(Timeframe.M5, 900, 6, 7, 4, 4.5), [sell_pool]))

    def test_sweep_detection_is_deduplicated_per_pool(self):
        detector = LiquiditySweepDetector()
        pool = LiquidityPool(Timeframe.M5, LiquidityType.BUY_SIDE, 10, [])
        self.assertIsNotNone(detector.check(bar(Timeframe.M5, 0, 9, 11, 8, 9.5), [pool]))
        self.assertIsNone(detector.check(bar(Timeframe.M5, 300, 9, 11, 8, 9.5), [pool]))

    def test_equal_highs_and_lows_remain_liquidity_pools(self):
        detector = LiquidityDetector(Timeframe.M5)
        highs = [
            SwingPoint(bar(Timeframe.M5, 0, 9, 10, 8, 9), SwingType.HIGH),
            SwingPoint(bar(Timeframe.M5, 600, 9, 10.002, 8, 9), SwingType.HIGH),
        ]
        lows = [
            SwingPoint(bar(Timeframe.M5, 300, 7, 8, 5, 6), SwingType.LOW),
            SwingPoint(bar(Timeframe.M5, 900, 7, 8, 5.001, 6), SwingType.LOW),
        ]
        pools = detector.detect(highs, lows)
        self.assertEqual(len(pools), 2)
        self.assertEqual({pool.liquidity_type for pool in pools},
                         {LiquidityType.BUY_SIDE, LiquidityType.SELL_SIDE})

    def test_bos_requires_body_close_beyond_swing_not_wick_only(self):
        detector = BOSDetector(Timeframe.M5)
        high_candle = bar(Timeframe.M5, 0, 9, 10, 8, 9)
        low_candle = bar(Timeframe.M5, 300, 6, 7, 5, 6)
        highs = [SwingPoint(high_candle, SwingType.HIGH)]
        lows = [SwingPoint(low_candle, SwingType.LOW)]
        self.assertIsNone(detector.check(bar(Timeframe.M5, 600, 9, 11, 8, 9.5), highs, lows, StructureBias.NEUTRAL))
        self.assertEqual(detector.check(bar(Timeframe.M5, 900, 9.5, 11, 9, 10.1), highs, lows,
                                         StructureBias.NEUTRAL).direction, BreakDirection.BULLISH)
        bear = BOSDetector(Timeframe.M5)
        self.assertIsNone(bear.check(bar(Timeframe.M5, 600, 6, 7, 4, 5.5), highs, lows, StructureBias.NEUTRAL))
        self.assertEqual(bear.check(bar(Timeframe.M5, 900, 5.5, 6, 4, 4.9), highs, lows,
                                    StructureBias.NEUTRAL).direction, BreakDirection.BEARISH)

    def test_be_waits_for_1_5r_and_is_idempotent(self):
        state = ProtectionState(BreakDirection.BULLISH, 100, 95, 0.2,
                                datetime.fromtimestamp(0, timezone.utc))
        self.assertIsNone(update_protection(state, 107.49))
        self.assertEqual(update_protection(state, 107.5), "breakeven_armed")
        self.assertEqual(state.protective_stop_price, 100.2)
        self.assertIsNone(update_protection(state, 108))
        self.assertEqual(update_protection(state, 100.2), "close")
        self.assertIsNone(update_protection(state, 100.2))

    def test_trailing_stop_only_moves_forward(self):
        state = ProtectionState(BreakDirection.BULLISH, 100, 95, 0.1,
                                datetime.fromtimestamp(0, timezone.utc),
                                breakeven_enabled=False, trailing_enabled=True)
        self.assertEqual(update_protection(state, 102, 98), "trailing_stop_advanced")
        self.assertIsNone(update_protection(state, 102, 97))
        self.assertEqual(state.protective_stop_price, 98)
        self.assertEqual(update_protection(state, 102, 99), "trailing_stop_advanced")
        self.assertEqual(state.protective_stop_price, 99)

    def test_wide_spread_rejects_new_entry_only(self):
        class FakeClient:
            async def buy_multiplier(self, **kwargs):
                raise AssertionError("wide-spread entry must not submit")

        with tempfile.TemporaryDirectory() as directory:
            history_file = Path(directory) / "history.csv"
            with patch.object(trade_history, "HISTORY_FILE", history_file), patch.object(db_sink, "record_trade_row"):
                trader = live_trader.LiveTrader("R_75", FakeClient(), max_entry_spread=0.1)
                trader.pending_signal = {
                    "direction": BreakDirection.BULLISH, "ticks_waited": 0,
                    "order_block": OrderBlock(Timeframe.M5, OrderBlockType.BULLISH, None, 110, 100, None),
                }
                tick = SimpleNamespace(quote=105, bid=104.5, ask=105.0,
                                       timestamp=datetime.fromtimestamp(1, timezone.utc), pip_size=2)
                asyncio.run(trader._check_pending_entry(tick))
                self.assertIsNone(trader.pending_signal)
                self.assertFalse(history_file.exists())

    def test_open_position_management_runs_independently_of_spread_gate(self):
        class FakeClient:
            async def close_contract(self, _contract_id):
                raise AssertionError("position is not at its protective stop")

        trader = live_trader.LiveTrader("R_75", FakeClient(), max_entry_spread=0.01)
        state = ProtectionState(BreakDirection.BULLISH, 100, 95, 0.2,
                                datetime.fromtimestamp(0, timezone.utc))
        trader.open_trade_protection["1"] = state
        tick = SimpleNamespace(quote=107.5, bid=100, ask=110,
                               timestamp=datetime.fromtimestamp(100, timezone.utc))
        asyncio.run(trader._manage_open_positions(tick))
        self.assertTrue(state.breakeven_armed)


if __name__ == "__main__":
    unittest.main()
