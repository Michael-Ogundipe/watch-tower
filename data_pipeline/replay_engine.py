from models import Candle, Timeframe
from market_structure import MarketStructure


class ReplayEngine:
    def __init__(self, service):
        self.service = service

    def replay(
        self,
        candles_by_timeframe: dict[Timeframe, list[Candle]],
        warmup: int = 100,
    ):
        for timeframe, candles in candles_by_timeframe.items():
            warmup_candles = candles[:warmup]

            self.service.structures[timeframe] = (
                MarketStructure(warmup_candles.copy())
            )

        replay_candles = []

        for timeframe, candles in candles_by_timeframe.items():
            replay_candles.extend(
                candles[warmup:]
            )

        replay_candles.sort(
            key=lambda candle: candle.timestamp
        )

        for candle in replay_candles:
            self.service.process_completed_candle(candle)