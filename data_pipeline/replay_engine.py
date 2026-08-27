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

            # Manage existing trade first
            trade_simulator.process_candle(candle)

            # Process market analysis
            signal = self.service.process_completed_candle(candle)

            # Create a new trade from a valid confluence
            if signal:
                trade = trade_simulator.create_trade(
                    signal,
                    candle.timestamp,
                )

                if trade:
                    print(
                        f"TRADE OPENED "
                        f"{trade.direction.value.upper()} "
                        f"at {trade.entry} "
                        f"SL {trade.stop_loss} "
                        f"TP {trade.take_profit}"
                    )