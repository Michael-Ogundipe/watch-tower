from models import (
    BreakDirection,
    BreakOfStructure,
    Candle,
    SwingType,
    Timeframe,
)


class BOSDetector:
    def __init__(self, timeframe: Timeframe):
        self.timeframe = timeframe
        self.breaks = []
        self.broken_highs = set()
        self.broken_lows = set()

    def check(self, candle: Candle, swing_highs, swing_lows):

        if not swing_highs or not swing_lows:
            return None

        latest_high = swing_highs[-1]
        latest_low = swing_lows[-1]

        # Bullish BOS
        if (
            candle.close > latest_high.candle.high
            and latest_high.candle.timestamp not in self.broken_highs
        ):
            bos = BreakOfStructure(
                timeframe=self.timeframe,
                direction=BreakDirection.BULLISH,
                broken_swing=latest_high,
                candle=candle,
            )

            self.breaks.append(bos)
            self.broken_highs.add(latest_high.candle.timestamp)

            return bos

        # Bearish BOS
        if (
            candle.close < latest_low.candle.low
            and latest_low.candle.timestamp not in self.broken_lows
        ):
            bos = BreakOfStructure(
                timeframe=self.timeframe,
                direction=BreakDirection.BEARISH,
                broken_swing=latest_low,
                candle=candle,
            )

            self.breaks.append(bos)
            self.broken_lows.add(latest_low.candle.timestamp)

            return bos

        return None