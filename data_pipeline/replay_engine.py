from models import Candle, Timeframe
from market_structure import MarketStructure
from trade_simulator import TradeSimulator


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

        trade_simulator = TradeSimulator()

        for candle in replay_candles:

            # Only M1 candles manage trade execution
            if candle.timeframe == Timeframe.M1:
                trade_simulator.process_candle(candle)

            # Run WatchTower analysis
            signal = self.service.process_completed_candle(candle)

            if signal:

                trade = trade_simulator.create_trade(
                    signal,
                    candle.timestamp,
                )

                if trade:
                    print(
                        f"REPLAY SIGNAL FOUND "
                        f"{signal['direction'].value.upper()} "
                        f"SCORE {signal['score']} "
                        f"TIME {candle.timestamp}"
                    )