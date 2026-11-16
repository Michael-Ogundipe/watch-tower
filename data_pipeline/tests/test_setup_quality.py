import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from live_trader import LiveTrader
import db_sink
from models import BreakDirection, BreakOfStructure, BreakType, Candle, SwingPoint, SwingType, Timeframe
from setup_quality import assess_displacement, premium_discount, score_setup_quality
from smc_refinement import rank_structural_targets
from trade_simulator import TradeSimulator


def bar(tf, epoch, open_, high, low, close):
    return Candle("R_75", tf, open_, high, low, close, datetime.fromtimestamp(epoch, timezone.utc))


class SetupQualityTests(unittest.TestCase):
    def test_quality_persistence_sql_parameters_match_schema_columns(self):
        with patch.object(db_sink, "_execute") as execute:
            db_sink.record_signal(1, {})
            db_sink.record_trade_row(1, {})
        for args, _kwargs in execute.call_args_list:
            sql, params = args[:2]
            self.assertEqual(sql.count("%s"), len(params))

    def test_strong_and_weak_displacement_use_only_prior_context(self):
        previous = [bar(Timeframe.M5, i * 300, 100, 101, 99, 100.2) for i in range(1, 8)]
        strong_candle = bar(Timeframe.M5, 2400, 100, 104, 96, 103.8)
        weak_candle = bar(Timeframe.M5, 2400, 100, 101, 99, 100.1)
        strong_break = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH, BreakType.BOS,
            SwingPoint(previous[-1], SwingType.HIGH), strong_candle)
        weak_break = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH, BreakType.BOS,
            SwingPoint(previous[-1], SwingType.HIGH), weak_candle)
        self.assertEqual(assess_displacement(strong_break, previous + [strong_candle]).category, "strong")
        self.assertEqual(assess_displacement(weak_break, previous + [weak_candle]).category, "weak")

    def test_displacement_is_directionally_symmetric(self):
        previous = [bar(Timeframe.M5, i * 300, 100, 101, 99, 100.1) for i in range(1, 6)]
        up = bar(Timeframe.M5, 1800, 100, 104, 96, 103.8)
        down = bar(Timeframe.M5, 1800, 100, 104, 96, 96.2)
        up_assessment = assess_displacement(BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH,
            BreakType.BOS, SwingPoint(previous[-1], SwingType.HIGH), up), previous + [up])
        down_assessment = assess_displacement(BreakOfStructure(Timeframe.M5, BreakDirection.BEARISH,
            BreakType.BOS, SwingPoint(previous[-1], SwingType.LOW), down), previous + [down])
        self.assertEqual(up_assessment.category, down_assessment.category)
        self.assertAlmostEqual(up_assessment.close_strength, down_assessment.close_strength)

    def test_premium_discount_uses_confirmed_historical_swings_only(self):
        raw = [(11, 9, 10), (15, 10, 14), (12, 8, 9), (11, 5, 6), (10, 6, 7)]
        candles = [bar(Timeframe.H1, i * 3600, 10, high, low, close)
                   for i, (high, low, close) in enumerate(raw)]
        structure = SimpleNamespace(candles=candles,
            swing_highs=[SwingPoint(candles[1], SwingType.HIGH)],
            swing_lows=[SwingPoint(candles[3], SwingType.LOW)])
        structures = {Timeframe.H1: structure}
        self.assertEqual(premium_discount(BreakDirection.BULLISH, structures,
            datetime.fromtimestamp(4 * 3600, timezone.utc))["state"], "unavailable")
        result = premium_discount(BreakDirection.BULLISH, structures,
            datetime.fromtimestamp(5 * 3600, timezone.utc))
        self.assertEqual(result["state"], "discount")
        self.assertEqual(result["adjustment"], 8)
        self.assertEqual(premium_discount(BreakDirection.BEARISH, structures,
            datetime.fromtimestamp(5 * 3600, timezone.utc))["adjustment"], -8)

    def test_quality_score_has_explainable_components(self):
        displacement = SimpleNamespace(score=75, as_dict=lambda: {"category": "strong", "score": 75})
        result = score_setup_quality(direction=BreakDirection.BULLISH,
            htf_poi={"poi": object(), "type": "order_block", "lower": 1, "upper": 2},
            sweep=SimpleNamespace(pool=SimpleNamespace(swing_points=[1, 2])),
            break_event=SimpleNamespace(break_type=BreakType.BOS), fvg=object(), order_block=object(),
            structures={Timeframe.M5: SimpleNamespace(candles=[])}, current_time=datetime.now(timezone.utc),
            target_info={"quality_score": 42, "potential_rr": 2.0, "source": "equal_liquidity", "level": 4},
            displacement=displacement, premium_discount_state={"state": "discount", "adjustment": 8})
        self.assertEqual(result["score"], sum(result["components"].values()))
        self.assertIn("displacement", result["components"])
        self.assertIn(result["category"], {"A+", "A", "B", "C"})

    def test_premium_and_discount_score_symmetrically_by_direction(self):
        # Price classification is independent; scoring applies directional polarity.
        displacement = SimpleNamespace(score=50, as_dict=lambda: {})
        common = dict(htf_poi={"poi": object(), "type": "order_block", "lower": 1, "upper": 2},
            sweep=SimpleNamespace(pool=SimpleNamespace(swing_points=[])),
            break_event=SimpleNamespace(break_type=BreakType.BOS), fvg=None, order_block=object(),
            structures={Timeframe.M5: SimpleNamespace(candles=[])}, current_time=datetime.now(timezone.utc),
            target_info={"quality_score": 0, "potential_rr": 1}, displacement=displacement)
        long_score = score_setup_quality(direction=BreakDirection.BEARISH,
            premium_discount_state={"state": "premium", "adjustment": 8}, **common)
        short_score = score_setup_quality(direction=BreakDirection.BULLISH,
            premium_discount_state={"state": "premium", "adjustment": -8}, **common)
        self.assertGreater(long_score["score"], short_score["score"])

    def test_target_rank_prefers_external_structure(self):
        m15 = bar(Timeframe.M15, 0, 104, 107, 103, 106)
        h1candles = [bar(Timeframe.H1, i * 3600, 108, high, 107, 108) for i, high in enumerate((109, 110, 109))]
        structures = {
            Timeframe.M15: SimpleNamespace(candles=[m15], swing_highs=[SwingPoint(m15, SwingType.HIGH)], swing_lows=[]),
            Timeframe.H1: SimpleNamespace(candles=h1candles,
                swing_highs=[SwingPoint(h1candles[1], SwingType.HIGH)], swing_lows=[]),
        }
        targets = rank_structural_targets(100, BreakDirection.BULLISH,
            datetime.fromtimestamp(20000, timezone.utc), structures, {}, {}, 0.01, stop_price=95)
        self.assertEqual(targets[0]["timeframe"], Timeframe.H1)

    def test_minimum_rr_gate_is_separate_from_quality_grade(self):
        signal = {"direction": BreakDirection.BULLISH,
            "order_block": SimpleNamespace(lower=95, upper=105),
            "quality": {"score": 20, "category": "C", "structural_target": 106, "tp_source": "swing"}}
        simulator = TradeSimulator("R_75", min_entry_rr=1.5)
        self.assertIsNone(simulator.create_trade(signal, datetime.now(timezone.utc)))
        simulator.min_entry_rr = 1.0
        self.assertIsNotNone(simulator.create_trade(signal, datetime.now(timezone.utc)))

    def test_backtest_records_realized_r_for_structural_target(self):
        signal = {"direction": BreakDirection.BULLISH,
            "order_block": SimpleNamespace(lower=95, upper=105),
            "quality": {"score": 85, "category": "A", "structural_target": 115,
                        "potential_rr": 3, "tp_source": "equal_liquidity"}}
        simulator = TradeSimulator("R_75")
        simulator.create_trade(signal, datetime.now(timezone.utc))
        simulator.process_candle(bar(Timeframe.M1, 1, 100, 101, 99, 100))
        simulator.process_candle(bar(Timeframe.M1, 61, 101, 116, 100, 115))
        self.assertEqual(simulator.completed_trades[0].result_r, 3.0)
        self.assertEqual(simulator.total_r, 3.0)

    def test_pending_setup_invalidates_on_htf_poi_failure(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.symbol = "R_75"
        trader.pending_signal = {"direction": BreakDirection.BULLISH, "setup_id": "setup-1",
            "htf_poi": {"poi": SimpleNamespace(status=SimpleNamespace(value="invalidated")), "type": "order_block"},
            "ltf_confirmation": None, "confluence_row": 3}
        trader.market_service = SimpleNamespace(bos_detectors={Timeframe.M5: SimpleNamespace(breaks=[])})
        trader._publish_runtime_status = lambda force=False: None
        with patch("live_trader.update_confluence") as update, patch("live_trader.emit_event") as event:
            self.assertTrue(trader._invalidate_pending_setup(bar(Timeframe.M5, 9000, 100, 101, 99, 100)))
        self.assertIsNone(trader.pending_signal)
        self.assertEqual(update.call_args.kwargs["setup_invalidation_reason"], "HTF_POI_INVALIDATED")
        self.assertEqual(event.call_args.args[0], "SIGNAL_INVALIDATED")

    def test_backtest_pending_trade_is_cancelled_when_poi_invalidates(self):
        poi = SimpleNamespace(status=SimpleNamespace(value="active"))
        confirmation_candle = bar(Timeframe.M5, 6000, 100, 102, 99, 101)
        confirmation = BreakOfStructure(Timeframe.M5, BreakDirection.BULLISH,
            BreakType.BOS, SwingPoint(confirmation_candle, SwingType.HIGH), confirmation_candle)
        signal = {"direction": BreakDirection.BULLISH,
            "order_block": SimpleNamespace(lower=95, upper=105),
            "htf_poi": {"poi": poi}, "ltf_confirmation": confirmation,
            "quality": {"score": 70, "category": "B", "structural_target": 110,
                        "potential_rr": 2, "tp_source": "swing"}}
        simulator = TradeSimulator("R_75")
        simulator.create_trade(signal, datetime.now(timezone.utc))
        poi.status = SimpleNamespace(value="invalidated")
        service = SimpleNamespace(bos_detectors={Timeframe.M5: SimpleNamespace(breaks=[])},
            structures={}, liquidity_detectors={}, fvg_detectors={})
        reason = simulator.invalidate_pending_setup(bar(Timeframe.M5, 6600, 100, 101, 99, 100), service)
        self.assertEqual(reason, "HTF_POI_INVALIDATED")
        self.assertIsNone(simulator.pending_trade)
        self.assertEqual(simulator.total_invalidated, 1)
