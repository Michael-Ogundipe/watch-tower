import asyncio

from models import Timeframe, SwingType
from market_structure import MarketStructure
from market_data_service import MarketDataService
from liquidity_detector import LiquidityDetector


async def main():
    service = MarketDataService("R_75")

    await service.initialize()

    h1_candles = service.get_candles(Timeframe.H1)

    structure = MarketStructure(h1_candles)

    liquidity_detector = LiquidityDetector(Timeframe.H1)

    pools = liquidity_detector.detect(
        structure.swing_highs,
        structure.swing_lows,
    )

    print(f"H1 liquidity pools: {len(pools)}")

    for pool in pools[-5:]:
        print(
            pool.liquidity_type,
            "pool:",
            pool.price,
            "points:",
            [
                point.candle.low
                if point.type == SwingType.LOW
                else point.candle.high
                for point in pool.swing_points
            ],
        )

    await service.run()


if __name__ == "__main__":
    asyncio.run(main())