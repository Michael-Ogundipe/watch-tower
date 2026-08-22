import asyncio

from models import Timeframe
from market_structure import MarketStructure
from market_data_service import MarketDataService


async def main():
    service = MarketDataService("R_75")

    await service.initialize()

    await service.run()


if __name__ == "__main__":
    asyncio.run(main())