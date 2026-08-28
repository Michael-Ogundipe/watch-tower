import asyncio

from models import Timeframe
from market_data_service import MarketDataService
from replay_engine import ReplayEngine


async def main():
    service = MarketDataService("R_75")

    await service.client.connect()

    candles_by_timeframe = {}

    # --------------------------------
    # Load historical data
    # --------------------------------

    for timeframe in Timeframe:
        candles = await service.client.get_candles(
            symbol="R_75",
            timeframe=timeframe,
            count=1000,
        )

        candles_by_timeframe[timeframe] = candles

    # --------------------------------
    # M1 defines the replay window
    # --------------------------------

    m1_candles = candles_by_timeframe[Timeframe.M1]

    replay_start = m1_candles[0].timestamp
    replay_end = m1_candles[-1].timestamp

    print(
        f"BACKTEST WINDOW "
        f"{replay_start} → {replay_end}"
    )

    # --------------------------------
    # Align all timeframes
    # --------------------------------

    for timeframe, candles in candles_by_timeframe.items():

        filtered_candles = [
            candle
            for candle in candles
            if candle.timestamp <= replay_end
        ]

        candles_by_timeframe[timeframe] = filtered_candles

        print(
            f"{timeframe.name}: "
            f"{len(filtered_candles)} candles "
            f"first={filtered_candles[0].timestamp} "
            f"last={filtered_candles[-1].timestamp}"
        )

    # --------------------------------
    # Replay
    # --------------------------------

    replay_engine = ReplayEngine(service)

    replay_engine.replay(candles_by_timeframe)


if __name__ == "__main__":
    asyncio.run(main())