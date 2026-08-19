from deriv_client import DerivClient
from candle_engine import CandleEngine
from models import Candle, Timeframe


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

    async def initialize(self):
        await self.client.connect()

        for timeframe, engine in self.engines.items():
            candles = await self.client.get_candles(
                symbol=self.symbol,
                timeframe=timeframe,
                count=100,
            )

            engine.initialize(candles)

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
                    print(
                        f"Completed "
                        f"{completed_candle.timeframe.name}: "
                        f"{completed_candle}"
                    )