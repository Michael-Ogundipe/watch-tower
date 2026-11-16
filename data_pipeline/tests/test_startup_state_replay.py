import unittest
from datetime import datetime, timedelta, timezone

from candle_engine import CandleEngine
from market_data_service import MarketDataService, TIMEFRAMES
from market_structure import MarketStructure
from models import Candle, Tick, Timeframe


def make_candle(timeframe, index, low, close=None):
    timestamp = datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(
        seconds=timeframe.value * index
    )
    close = low + 2 if close is None else close
    return Candle(
        symbol="R_75",
        timeframe=timeframe,
        open=low + 1,
        high=low + 5,
        low=low,
        close=close,
        timestamp=timestamp,
    )


class StartupStateReplayTests(unittest.TestCase):
    @staticmethod
    def historical_sequence():
        result = {}
        for timeframe in TIMEFRAMES:
            if timeframe == Timeframe.M5:
                # Two equal confirmed swing lows form a sell-side pool;
                # candle 7 sweeps it and closes back above the pool.
                lows = [103, 100, 102, 104, 100, 102, 104, 98, 101, 103, 102]
                result[timeframe] = [
                    make_candle(
                        timeframe,
                        index,
                        low,
                        close=101 if index == 7 else low + 2,
                    )
                    for index, low in enumerate(lows)
                ]
            else:
                lows = [100 + (index % 4) for index in range(11)]
                result[timeframe] = [
                    make_candle(timeframe, index, low)
                    for index, low in enumerate(lows)
                ]
        return result

    @staticmethod
    def service_with_seeded_structures(candles_by_timeframe):
        service = MarketDataService("R_75")
        for timeframe, candles in candles_by_timeframe.items():
            service.engines[timeframe].initialize(
                candles,
                seed_latest_as_current=False,
            )
            service.structures[timeframe] = MarketStructure(candles[:2])
        return service

    @staticmethod
    def continuous_replay(service, candles_by_timeframe):
        order = {timeframe: index for index, timeframe in enumerate(TIMEFRAMES)}
        events = [
            (
                MarketDataService.candle_completion_time(candle),
                order[timeframe],
                candle,
            )
            for timeframe, candles in candles_by_timeframe.items()
            for candle in candles[2:]
        ]
        for _, _, candle in sorted(events, key=lambda event: (event[0], event[1])):
            service.process_completed_candle(candle, emit_signal=False)

    def test_warmed_detectors_match_continuous_processing(self):
        candles_by_timeframe = self.historical_sequence()

        continuously_running = self.service_with_seeded_structures(
            candles_by_timeframe
        )
        self.continuous_replay(continuously_running, candles_by_timeframe)

        freshly_started = self.service_with_seeded_structures(
            candles_by_timeframe
        )
        freshly_started._replay_historical_state(candles_by_timeframe)

        for timeframe in TIMEFRAMES:
            self.assertEqual(
                continuously_running.structures[timeframe].candles,
                freshly_started.structures[timeframe].candles,
            )
            self.assertEqual(
                continuously_running.structures[timeframe].get_bias(),
                freshly_started.structures[timeframe].get_bias(),
            )
            self.assertEqual(
                continuously_running.bos_detectors[timeframe].breaks,
                freshly_started.bos_detectors[timeframe].breaks,
            )
            self.assertEqual(
                continuously_running.fvg_detectors[timeframe].fvgs,
                freshly_started.fvg_detectors[timeframe].fvgs,
            )
            self.assertEqual(
                continuously_running.order_block_detectors[timeframe].order_blocks,
                freshly_started.order_block_detectors[timeframe].order_blocks,
            )
            self.assertEqual(
                continuously_running.liquidity_sweep_detectors[timeframe].sweeps,
                freshly_started.liquidity_sweep_detectors[timeframe].sweeps,
            )
            self.assertEqual(
                continuously_running.liquidity_detectors[timeframe].pools,
                freshly_started.liquidity_detectors[timeframe].pools,
            )

        sweeps = freshly_started.liquidity_sweep_detectors[Timeframe.M5].sweeps
        expected_sweep_time = candles_by_timeframe[Timeframe.M5][7].timestamp
        self.assertTrue(
            any(sweep.candle.timestamp == expected_sweep_time for sweep in sweeps)
        )
        self.assertEqual(
            continuously_running.last_confluence_signal,
            freshly_started.last_confluence_signal,
        )

        # Both states make the same real ConfluenceEngine decision when the
        # next M5 candle is evaluated. No broker/execution code is involved.
        next_candle = make_candle(Timeframe.M5, 11, 102)
        live_decision = continuously_running.process_completed_candle(next_candle)
        warmed_decision = freshly_started.process_completed_candle(next_candle)
        self.assertEqual(live_decision, warmed_decision)

    def test_historical_last_candle_is_not_reemitted_by_first_live_tick(self):
        history = [make_candle(Timeframe.M5, index, 100 + index) for index in range(3)]
        engine = CandleEngine(Timeframe.M5)
        engine.initialize(history, seed_latest_as_current=False)

        tick = Tick(
            symbol="R_75",
            quote=105,
            bid=105,
            ask=105,
            timestamp=history[-1].timestamp + timedelta(seconds=Timeframe.M5.value),
            pip_size=2,
        )
        self.assertIsNone(engine.process_tick(tick))


if __name__ == "__main__":
    unittest.main()
