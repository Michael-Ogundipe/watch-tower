import asyncio

from market_data_service import MarketDataService
from models import Timeframe


async def main():
    service = MarketDataService("R_75")

    await service.initialize()
    
    h1_candles = service.get_candles(Timeframe.H1)

    print(f"H1 candles available: {len(h1_candles)}")
    print(f"Latest H1 candle: {h1_candles[-1]}")

    await service.run()


if __name__ == "__main__":
    asyncio.run(main())