from models import (
    Candle,
    FairValueGap,
    FVGType,
    Timeframe,
)


class FVGDetector:

    def __init__(self, timeframe: Timeframe):
        self.timeframe = timeframe
        self.fvgs: list[FairValueGap] = []

    def check(self, candles: list[Candle]):
        if len(candles) < 3:
            return None

        first = candles[-3]
        middle = candles[-2]
        third = candles[-1]

        # Bullish FVG
        if first.high < third.low:
            fvg = FairValueGap(
                timeframe=self.timeframe,
                fvg_type=FVGType.BULLISH,
                upper=third.low,
                lower=first.high,
                candles=[first, middle, third],
            )

            self.fvgs.append(fvg)
            return fvg

        # Bearish FVG
        if first.low > third.high:
            fvg = FairValueGap(
                timeframe=self.timeframe,
                fvg_type=FVGType.BEARISH,
                upper=first.low,
                lower=third.high,
                candles=[first, middle, third],
            )

            self.fvgs.append(fvg)
            return fvg

        return None


