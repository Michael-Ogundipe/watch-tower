from models import (
    LiquidityPool,
    LiquidityType,
    SwingPoint,
    Timeframe,
)


class LiquidityDetector:
    def __init__(
        self,
        timeframe: Timeframe,
        tolerance: float = 0.0005,
    ):
        self.timeframe = timeframe
        self.tolerance = tolerance
        self.pools: list[LiquidityPool] = []

    def detect(
        self,
        swing_highs: list[SwingPoint],
        swing_lows: list[SwingPoint],
    ):
        self.pools = []

        self._detect_equal_highs(swing_highs)
        self._detect_equal_lows(swing_lows)

        return self.pools


    def _detect_equal_highs(self, swings: list[SwingPoint]):
        self._cluster_swings(
            swings=swings,
            liquidity_type=LiquidityType.BUY_SIDE,
            price_getter=lambda swing: swing.candle.high,
        )


    def _detect_equal_lows(self, swings: list[SwingPoint]):
        self._cluster_swings(
            swings=swings,
            liquidity_type=LiquidityType.SELL_SIDE,
            price_getter=lambda swing: swing.candle.low,
        )


    def _is_near(self, first: float, second: float) -> bool:
        difference = abs(first - second)

        return difference <= first * self.tolerance    


    def _cluster_swings(
        self,
        swings: list[SwingPoint],
        liquidity_type: LiquidityType,
        price_getter,
    ):
        clusters = []

        for swing in swings:
            price = price_getter(swing)

            matching_cluster = None

            for cluster in clusters:
                cluster_price = cluster["price"]

                if self._is_near(price, cluster_price):
                    matching_cluster = cluster
                    break

            if matching_cluster:
                matching_cluster["swings"].append(swing)

                prices = [
                    price_getter(point)
                    for point in matching_cluster["swings"]
                ]

                matching_cluster["price"] = sum(prices) / len(prices)

            else:
                clusters.append(
                    {
                        "price": price,
                        "swings": [swing],
                    }
                )

        for cluster in clusters:
            if len(cluster["swings"]) < 2:
                continue

            self.pools.append(
                LiquidityPool(
                    timeframe=self.timeframe,
                    liquidity_type=liquidity_type,
                    price=cluster["price"],
                    swing_points=cluster["swings"],
                )
            )    


                    