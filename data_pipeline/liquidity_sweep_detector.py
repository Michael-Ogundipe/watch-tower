from models import (
    Candle,
    LiquidityPool,
    LiquidityType,
    LiquiditySweep
)

class LiquiditySweepDetector:

    def __init__(self):
        self.sweeps: list[LiquiditySweep] = []
        self.swept_pools = set()

    def check(
        self,
        candle: Candle,
        pools: list[LiquidityPool],
    ):
        for pool in pools:

            pool_key = (
                pool.liquidity_type,
                pool.price,
            )

            if pool_key in self.swept_pools:
                continue

            # Buy-side liquidity sweep
            if pool.liquidity_type == LiquidityType.BUY_SIDE:
                if (
                    candle.high > pool.price
                    and candle.close < pool.price
                ):
                    return self._record_sweep(candle, pool)

            # Sell-side liquidity sweep
            if pool.liquidity_type == LiquidityType.SELL_SIDE:
                if (
                    candle.low < pool.price
                    and candle.close > pool.price
                ):
                    return self._record_sweep(candle, pool)

        return None


    def _record_sweep(
        self,
        candle: Candle,
        pool: LiquidityPool,
    ):
        sweep = LiquiditySweep(
            timeframe=pool.timeframe,
            liquidity_type=pool.liquidity_type,
            pool=pool,
            candle=candle,
        )

        self.sweeps.append(sweep)

        self.swept_pools.add(
            (
                pool.liquidity_type,
                pool.price,
            )
        )

        return sweep


    def get_latest_sweep(self):
        if not self.sweeps:
            return None

        return self.sweeps[-1]

