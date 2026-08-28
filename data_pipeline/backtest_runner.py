import asyncio

from models import Timeframe
from market_data_service import MarketDataService
from replay_engine import ReplayEngine


async def main():
    service = MarketDataService("R_75")

    await service.client.connect()

    candles_by_timeframe = {}

    for timeframe in Timeframe:
        candles = await service.client.get_candles(
            symbol="R_75",
            timeframe=timeframe,
            count=1000,
        )

        candles_by_timeframe[timeframe] = candles

        print(
            f"Loaded {len(candles)} "
            f"{timeframe.name} candles"
        )

        print(
            f"{timeframe.name}: "
            f"first={candles[0].timestamp}, "
            f"last={candles[-1].timestamp}"
        )

    replay_engine = ReplayEngine(service)

    replay_engine.replay(candles_by_timeframe)


if __name__ == "__main__":
    asyncio.run(main())