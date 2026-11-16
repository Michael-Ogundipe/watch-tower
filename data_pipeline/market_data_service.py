from datetime import datetime, timedelta, timezone

from deriv_client import DerivClient
from candle_engine import CandleEngine
from models import Candle, Timeframe
from market_structure import MarketStructure
from bos_detector import BOSDetector
from liquidity_sweep_detector import LiquiditySweepDetector
from liquidity_detector import LiquidityDetector
from fvg_detector import FVGDetector
from order_block_detector import OrderBlockDetector
from confluence_engine import ConfluenceEngine



TIMEFRAMES = [
    Timeframe.M1,
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
]


class MarketDataService:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.client = DerivClient()

        self.engines = {
            timeframe: CandleEngine(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.structures = {}
        self.bos_detectors = {
            timeframe: BOSDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.order_block_detectors = {
            timeframe: OrderBlockDetector(timeframe)
            for timeframe in Timeframe
        }

        self.liquidity_sweep_detectors = {
            timeframe: LiquiditySweepDetector()
            for timeframe in TIMEFRAMES
        }

        self.liquidity_detectors = {
            timeframe: LiquidityDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.fvg_detectors = {
            timeframe: FVGDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.confluence_engine = ConfluenceEngine(
            self.structures,
            self.fvg_detectors,
            self.order_block_detectors,
            self.liquidity_sweep_detectors,
            self.bos_detectors,
            self.liquidity_detectors,
        )

        self.last_confluence_signal = None

    async def initialize(self):
        await self.client.connect()

        candles_by_timeframe = {}
        initialized_at = datetime.now(timezone.utc)

        for timeframe, engine in self.engines.items():
            candles = await self.client.get_candles(
                symbol=self.symbol,
                timeframe=timeframe,
                count=100,
            )

            # Deriv's latest candle can still be forming. Only closed bars
            # belong in historical state reconstruction.
            candles = [
                candle
                for candle in candles
                if self.candle_completion_time(candle) <= initialized_at
            ]
            candles_by_timeframe[timeframe] = candles

            # Keep history available to callers, but do not seed the last
            # historical candle as CandleEngine.current_candle. The detector
            # state below has already consumed it; seeding it would emit that
            # same candle again on the first live tick.
            engine.initialize(candles, seed_latest_as_current=False)

            # Seed the two bars needed to confirm the next swing, then replay
            # all subsequent bars through the same incremental live path.
            self.structures[timeframe] = MarketStructure(candles[:2])

            print(
                f"Loaded {len(candles)} "
                f"{timeframe.name} candles"
            )

        self._replay_historical_state(candles_by_timeframe)
        replayed = sum(len(candles) for candles in candles_by_timeframe.values())
        print(
            f"Reconstructed strategy state from {replayed} closed historical candles"
        )

    @staticmethod
    def candle_completion_time(candle):
        return candle.timestamp + timedelta(seconds=candle.timeframe.value)

    def _replay_historical_state(self, candles_by_timeframe):
        """Rebuild stateful detectors chronologically without evaluating trades."""
        timeframe_order = {
            timeframe: index
            for index, timeframe in enumerate(self.engines)
        }
        replay = []

        for timeframe, candles in candles_by_timeframe.items():
            # The first two candles seeded MarketStructure above. Live
            # processing starts on the third bar, when a swing can first be
            # confirmed. The seeded bars still remain available to FVG logic.
            for candle in candles[2:]:
                replay.append((self.candle_completion_time(candle), timeframe, candle))

        replay.sort(key=lambda item: (item[0], timeframe_order[item[1]]))

        for _, _, candle in replay:
            self.process_completed_candle(
                candle,
                emit_signal=False,
            )

    def get_candles(self, timeframe: Timeframe) -> list[Candle]:
        engine = self.engines[timeframe]

        return engine.candles


    def get_structure_state(self, timeframe: Timeframe):
        structure = self.structures.get(timeframe)

        if structure is None:
            return None

        return structure.get_state()


    def process_completed_candle(
        self,
        completed_candle,
        *,
        evaluate_confluence: bool = True,
        emit_signal: bool = True,
    ):
        timeframe = completed_candle.timeframe

        structure = self.structures[timeframe]

        structure.process_candle(completed_candle)

        # FVG
        fvg_detector = self.fvg_detectors[timeframe]

        fvg = fvg_detector.check(
            structure.candles,
        )

        # Liquidity
        liquidity_detector = self.liquidity_detectors[timeframe]

        pools = liquidity_detector.detect(
            structure.swing_highs,
            structure.swing_lows,
        )

        sweep_detector = self.liquidity_sweep_detectors[timeframe]

        sweep = sweep_detector.check(
            completed_candle,
            pools,
        )

        # Order Block lifecycle
        order_block_detector = self.order_block_detectors[timeframe]

        order_block_detector.update_status(
            completed_candle
        )

        # BOS / CHOCH
        bos_detector = self.bos_detectors[timeframe]

        bos = bos_detector.check(
            completed_candle,
            structure.swing_highs,
            structure.swing_lows,
            structure.get_bias(),
        )

        if bos:
            order_block_detector.check(
                bos,
                structure.candles,
            )

        # Confluence is evaluated only when an M5 candle completes.
        if timeframe != Timeframe.M5 or not evaluate_confluence:
            return None

        analysis = self.confluence_engine.analyze(
            completed_candle.timestamp + timedelta(seconds=timeframe.value)
        )

        if analysis:
            
            signal_key = analysis["setup_id"]

            if signal_key != self.last_confluence_signal:
                self.last_confluence_signal = signal_key

                if emit_signal:
                    print(
                        f"CONFLUENCE "
                        f"{analysis['direction'].value.upper()} "
                        f"SCORE {analysis['score']}"
                    )

                    for reason in analysis["reasons"]:
                        print(f"  ✓ {reason}")

                    return analysis

        return None
    

    

    async def run(self):
        async for tick in self.client.subscribe_ticks(self.symbol):

            for engine in self.engines.values():
                completed_candle = engine.process_tick(tick)

                if completed_candle:
                    self.process_completed_candle(
                        completed_candle
                    )
