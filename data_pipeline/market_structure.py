from models import *


class MarketStructure:
    def __init__(self, candles: list[Candle]):
        self.candles = list(candles)
        
        self.swing_highs = self.find_swing_highs()
        self.swing_lows = self.find_swing_lows()
        self.structure_points = []
        self.build_structure()


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

    def process_candle(self, candle: Candle):
        self.candles.append(candle)

        if len(self.candles) < 3:
            return

        previous = self.candles[-3]
        current = self.candles[-2]
        next_candle = self.candles[-1]

        # Check for swing high
        if (
            current.high > previous.high
            and current.high > next_candle.high
        ):
            swing = SwingPoint(
                candle=current,
                type=SwingType.HIGH,
            )

            self._add_structure_point(swing)
            self.swing_highs.append(swing)

        if (
            current.low < previous.low
            and current.low < next_candle.low
        ):
            swing = SwingPoint(
                candle=current,
                type=SwingType.LOW,
            )

            self._add_structure_point(swing)
            self.swing_lows.append(swing)

    def get_latest_swing_high(self):
        if not self.swing_highs:
            return None

        return self.swing_highs[-1]


    def get_latest_swing_low(self):
        if not self.swing_lows:
            return None

        return self.swing_lows[-1]


    def classify_swing(
        self,
        previous: SwingPoint,
        current: SwingPoint,
    ) -> StructureType | None:

        if previous.type != current.type:
            return None

        if current.type == SwingType.HIGH:
            if current.candle.high > previous.candle.high:
                return StructureType.HH

            return StructureType.LH

        if current.type == SwingType.LOW:
            if current.candle.low > previous.candle.low:
                return StructureType.HL

            return StructureType.LL

        return None


    def build_structure(self):
        swings = sorted(
            self.swing_highs + self.swing_lows,
            key=lambda swing: swing.candle.timestamp,
        )

        previous_high = None
        previous_low = None

        for swing in swings:

            if swing.type == SwingType.HIGH:
                if previous_high is not None:
                    structure_type = self.classify_swing(
                        previous_high,
                        swing,
                    )

                    if structure_type:
                        self.structure_points.append(
                            StructurePoint(
                                swing=swing,
                                structure_type=structure_type,
                            )
                        )

                previous_high = swing

            else:
                if previous_low is not None:
                    structure_type = self.classify_swing(
                        previous_low,
                        swing,
                    )

                    if structure_type:
                        self.structure_points.append(
                            StructurePoint(
                                swing=swing,
                                structure_type=structure_type,
                            )
                        )

                previous_low = swing

    

    def _add_structure_point(self, swing: SwingPoint):
        if swing.type == SwingType.HIGH:
            previous = self.swing_highs[-1] if self.swing_highs else None
        else:
            previous = self.swing_lows[-1] if self.swing_lows else None

        if previous is None:
            return

        structure_type = self.classify_swing(previous, swing)

        if structure_type:
            structure_point = StructurePoint(
                swing=swing,
                structure_type=structure_type,
            )

            self.structure_points.append(structure_point)

            print(
                f"New structure: "
                f"{structure_type.value.upper()} "
                f"at {swing.candle.timestamp}"
            )


    def get_bias(self) -> StructureBias:
        if not self.structure_points:
            return StructureBias.NEUTRAL

        latest_high = None
        latest_low = None

        for point in reversed(self.structure_points):
            if point.swing.type == SwingType.HIGH and latest_high is None:
                latest_high = point

            if point.swing.type == SwingType.LOW and latest_low is None:
                latest_low = point

            if latest_high and latest_low:
                break

        if latest_high is None or latest_low is None:
            return StructureBias.NEUTRAL

        if latest_high.structure_type == StructureType.HH:
            if latest_low.structure_type == StructureType.HL:
                return StructureBias.BULLISH

        if latest_high.structure_type == StructureType.LH:
            if latest_low.structure_type == StructureType.LL:
                return StructureBias.BEARISH

        return StructureBias.NEUTRAL


    def get_state(self) -> MarketStructureState:
        latest_high = None
        latest_low = None

        for point in reversed(self.structure_points):
            if point.swing.type == SwingType.HIGH and latest_high is None:
                latest_high = point.swing

            if point.swing.type == SwingType.LOW and latest_low is None:
                latest_low = point.swing

            if latest_high and latest_low:
                break

        return MarketStructureState(
            timeframe=latest_high.candle.timeframe
            if latest_high
            else self.candles[-1].timeframe,
            bias=self.get_bias(),
            latest_high=latest_high,
            latest_low=latest_low,
            structure_points=self.structure_points.copy(),
        )


