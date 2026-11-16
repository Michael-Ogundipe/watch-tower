from models import (
    LiquidityPool,
    LiquidityType,
    SwingPoint,
    Timeframe,
)


class LiquidityDetector:
    """
    Incremental equivalent of the original liquidity detector.

    MarketStructure only appends swing points, so rebuilding every cluster
    from scratch on every candle is unnecessary. We keep the same
    order-sensitive clustering algorithm and only process newly appended
    swings. This changes execution cost, not strategy behavior.
    """

    def __init__(
        self,
        timeframe: Timeframe,
        tolerance: float = 0.0005,
    ):
        self.timeframe = timeframe
        self.tolerance = tolerance
        self.pools: list[LiquidityPool] = []

        self._high_clusters = []
        self._low_clusters = []
        self._high_count = 0
        self._low_count = 0

    def detect(
        self,
        swing_highs: list[SwingPoint],
        swing_lows: list[SwingPoint],
    ):
        # If a caller replaces/shrinks the source lists, rebuild so the
        # detector remains compatible with the original implementation.
        rebuilt = False
        if (
            len(swing_highs) < self._high_count
            or len(swing_lows) < self._low_count
        ):
            self._high_clusters = []
            self._low_clusters = []
            self._high_count = 0
            self._low_count = 0
            rebuilt = True

        previous_high_count = self._high_count
        previous_low_count = self._low_count

        for swing in swing_highs[self._high_count:]:
            self._add_to_clusters(
                self._high_clusters,
                swing,
                LiquidityType.BUY_SIDE,
                lambda point: point.candle.high,
            )

        for swing in swing_lows[self._low_count:]:
            self._add_to_clusters(
                self._low_clusters,
                swing,
                LiquidityType.SELL_SIDE,
                lambda point: point.candle.low,
            )

        self._high_count = len(swing_highs)
        self._low_count = len(swing_lows)

        # Pools only change when MarketStructure appends a swing. Rebuilding
        # the complete pool list on every replay candle was wasted work.
        if rebuilt or self._high_count != previous_high_count or self._low_count != previous_low_count:
            self.pools = self._build_pools()
        return self.pools

    def _is_near(self, first: float, second: float) -> bool:
        difference = abs(first - second)
        return difference <= first * self.tolerance

    def _add_to_clusters(
        self,
        clusters,
        swing: SwingPoint,
        liquidity_type: LiquidityType,
        price_getter,
    ):
        price = price_getter(swing)

        for cluster in clusters:
            if self._is_near(price, cluster["price"]):
                cluster["swings"].append(swing)

                # Preserve the original implementation's running-average
                # cluster price exactly.
                cluster["price"] = sum(
                    price_getter(point)
                    for point in cluster["swings"]
                ) / len(cluster["swings"])

                return

        clusters.append(
            {
                "price": price,
                "swings": [swing],
            }
        )

    def _build_pools(self):
        pools = []

        # The original detector emits all high pools first, then all low
        # pools. Keep that exact ordering.
        for clusters, liquidity_type in (
            (self._high_clusters, LiquidityType.BUY_SIDE),
            (self._low_clusters, LiquidityType.SELL_SIDE),
        ):
            for cluster in clusters:
                if len(cluster["swings"]) < 2:
                    continue

                pools.append(
                    LiquidityPool(
                        timeframe=self.timeframe,
                        liquidity_type=liquidity_type,
                        price=cluster["price"],
                        swing_points=cluster["swings"],
                    )
                )

        return pools
