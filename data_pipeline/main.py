import asyncio

from market_data_service import MarketDataService
from models import Timeframe
from market_structure import MarketStructure


async def main():
    service = MarketDataService("R_75")

    await service.initialize()

    h1_candles = service.get_candles(Timeframe.H1)

    structure = MarketStructure(h1_candles)

    swing_highs = structure.find_swing_highs()
    swing_lows = structure.find_swing_lows()

    print(f"Swing highs: {len(swing_highs)}")
    print(f"Swing lows: {len(swing_lows)}")

    if len(swing_highs) >= 2:
        result = structure.classify_swing(
            swing_highs[-2],
            swing_highs[-1],
        )

        print("Latest high structure:", result)


    if len(swing_lows) >= 2:
        result = structure.classify_swing(
            swing_lows[-2],
            swing_lows[-1],
        )

        print("Latest low structure:", result)
        


    await service.run()


if __name__ == "__main__":
    asyncio.run(main())