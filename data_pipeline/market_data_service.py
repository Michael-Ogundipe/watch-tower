from deriv_client import DerivClient
from candle_engine import CandleEngine
from models import Candle, Timeframe
from market_structure import MarketStructure
from bos_detector import BOSDetector


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

    async def initialize(self):
        await self.client.connect()

        for timeframe, engine in self.engines.items():
            candles = await self.client.get_candles(
                symbol=self.symbol,
                timeframe=timeframe,
                count=100,
            )

            engine.initialize(candles)

            self.structures[timeframe] = MarketStructure(candles)

            print(
                f"Loaded {len(candles)} "
                f"{timeframe.name} candles"
            )

    def get_candles(self, timeframe: Timeframe) -> list[Candle]:
        engine = self.engines[timeframe]

        return engine.candles

    async def run(self):
        async for tick in self.client.subscribe_ticks(self.symbol):

            for engine in self.engines.values():
                completed_candle = engine.process_tick(tick)

                if completed_candle:
                    timeframe = completed_candle.timeframe

                    structure = self.structures[timeframe]

                    structure.process_candle(completed_candle)

                    bos_detector = self.bos_detectors[timeframe]

                    bos = bos_detector.check(
                        completed_candle,
                        structure.swing_highs,
                        structure.swing_lows,
                    )

                    if bos:
                        print(
                            f"BOS {bos.direction.value.upper()} "
                            f"on {timeframe.name} "
                            f"at {bos.candle.timestamp}"
                        )

                    latest_high = structure.get_latest_swing_high()
                    latest_low = structure.get_latest_swing_low()

                    print(
                        f"Completed {timeframe.name}: "
                        f"{completed_candle}"
                    )

                    if latest_high:
                        print(
                            f"{timeframe.name} latest swing high: "
                            f"{latest_high}"
                        )

                    if latest_low:
                        print(
                            f"{timeframe.name} latest swing low: "
                            f"{latest_low}"
                        )