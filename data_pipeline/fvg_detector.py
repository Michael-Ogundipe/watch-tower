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

        # Update existing FVGs
        for fvg in self.fvgs:
            old_status = fvg.status

            self._update_fvg_status(fvg, current)

            if fvg.status != old_status:
                return fvg

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


    def _update_fvg_status(
        self,
        fvg: FairValueGap,
        candle: Candle,
    ):
        if fvg.status == FVGStatus.FILLED:
            return

        if fvg.fvg_type == FVGType.BULLISH:

            if candle.low <= fvg.lower:
                fvg.status = FVGStatus.FILLED

            elif candle.low <= fvg.upper:
                fvg.status = FVGStatus.MITIGATED

        elif fvg.fvg_type == FVGType.BEARISH:

            if candle.high >= fvg.upper:
                fvg.status = FVGStatus.FILLED

            elif candle.high >= fvg.lower:
                fvg.status = FVGStatus.MITIGATED


    def get_active_fvgs(self):
        return [
            fvg
            for fvg in self.fvgs
            if fvg.status != FVGStatus.FILLED
        ]