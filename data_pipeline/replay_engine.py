from models import Candle, Timeframe
from market_structure import MarketStructure
from trade_simulator import TradeSimulator


class ReplayEngine:
    def __init__(self, service):
        self.service = service

    def replay(
        self,
        candles_by_timeframe: dict[Timeframe, list[Candle]],
    ):

        # --------------------------------
        # M1 defines the replay window
        # --------------------------------

        m1_candles = candles_by_timeframe[Timeframe.M1]

        replay_start = m1_candles[0].timestamp
        replay_end = m1_candles[-1].timestamp

        # --------------------------------
        # Initialize each timeframe
        # with candles before replay_start
        # --------------------------------

        for timeframe, candles in candles_by_timeframe.items():

            warmup_candles = [
                candle
                for candle in candles
                if candle.timestamp < replay_start
            ]

            self.service.structures[timeframe] = (
                MarketStructure(warmup_candles.copy())
            )

        # --------------------------------
        # Build synchronized replay stream
        # --------------------------------

        replay_candles = []

        for timeframe, candles in candles_by_timeframe.items():

            timeframe_replay = [
                candle
                for candle in candles
                if (
                    replay_start
                    <= candle.timestamp
                    <= replay_end
                )
            ]

            replay_candles.extend(timeframe_replay)

        replay_candles.sort(
            key=lambda candle: candle.timestamp
        )

        # --------------------------------
        # Trade simulator
        # --------------------------------

        trade_simulator = TradeSimulator()

        # --------------------------------
        # Replay
        # --------------------------------

        for candle in replay_candles:

            # M1 controls trade execution
            if candle.timeframe == Timeframe.M1:
                trade_simulator.process_candle(candle)

            # Update market analysis
            signal = self.service.process_completed_candle(
                candle
            )

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