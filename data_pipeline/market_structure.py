from models import Candle, SwingPoint, SwingType


class MarketStructure:
    def __init__(self, candles: list[Candle]):
        self.candles = candles


    # Finds confirmed swing highs using the candle before and after each candidate.
    def find_swing_highs(self):
        swing_highs = []

        for i in range(1, len(self.candles) - 1):
            previous = self.candles[i - 1]
            current = self.candles[i]
            next_candle = self.candles[i + 1]

            if (
                current.high > previous.high
                and current.high > next_candle.high
            ):
                swing_highs.append(
                    SwingPoint(
                        candle=current,
                        type=SwingType.HIGH,
                    )
                )

        return swing_highs


    def find_swing_lows(self):
        swing_lows = []

        for i in range(1, len(self.candles) - 1):
            previous = self.candles[i - 1]
            current = self.candles[i]
            next_candle = self.candles[i + 1]

            if (
                current.low < previous.low
                and current.low < next_candle.low
            ):
                swing_lows.append(
                    SwingPoint(
                        candle=current,
                        type=SwingType.LOW,
                    )
                )

        return swing_lows

    

    