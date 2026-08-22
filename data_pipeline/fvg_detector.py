from models import (
    Candle,
    FairValueGap,
    FVGType,
    Timeframe,
    FVGStatus,
)


class FVGDetector:

    def __init__(self, timeframe: Timeframe):
        self.timeframe = timeframe
        self.fvgs: list[FairValueGap] = []

    def check(self, candles: list[Candle]):
        if len(candles) < 3:
            return None

        current = candles[-1]

        # First update existing FVGs
        self._update_existing_fvgs(current)

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


    def _update_existing_fvgs(self, candle: Candle):

        for fvg in self.fvgs:

            if fvg.status == FVGStatus.FILLED:
                continue

            # Bullish FVG
            if fvg.fvg_type == FVGType.BULLISH:

                # Completely filled
                if candle.low <= fvg.lower:
                    fvg.status = FVGStatus.FILLED

                # Price has entered the zone
                elif candle.low <= fvg.upper:
                    fvg.status = FVGStatus.MITIGATED

            # Bearish FVG
            elif fvg.fvg_type == FVGType.BEARISH:

                # Completely filled
                if candle.high >= fvg.upper:
                    fvg.status = FVGStatus.FILLED

                # Price has entered the zone
                elif candle.high >= fvg.lower:
                    fvg.status = FVGStatus.MITIGATED
