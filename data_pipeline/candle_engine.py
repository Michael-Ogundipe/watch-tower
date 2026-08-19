from datetime import datetime

from models import Candle, Tick, Timeframe


class CandleEngine:
    def __init__(self, timeframe: Timeframe):
        self.timeframe = timeframe
        self.candles = []
        self.current_candle = None
    

    def _get_candle_start(self, timestamp: datetime) -> datetime:
        timestamp_seconds = int(timestamp.timestamp())

        timeframe_seconds = self.timeframe.value

        candle_start_seconds = (
            timestamp_seconds // timeframe_seconds
        ) * timeframe_seconds

        return datetime.fromtimestamp(
            candle_start_seconds,
            tz=timestamp.tzinfo,
        )


    def process_tick(self, tick: Tick) -> Candle | None:
        candle_start = self._get_candle_start(tick.timestamp)

        # First tick — create the first candle
        if self.current_candle is None:
            self.current_candle = Candle(
                symbol=tick.symbol,
                timeframe=self.timeframe,
                open=tick.quote,
                high=tick.quote,
                low=tick.quote,
                close=tick.quote,
                timestamp=candle_start,
            )

            return None

        # Tick belongs to a new candle
        if candle_start > self.current_candle.timestamp:
            completed_candle = self.current_candle
            self.candles.append(completed_candle)

            self.current_candle = Candle(
                symbol=tick.symbol,
                timeframe=self.timeframe,
                open=tick.quote,
                high=tick.quote,
                low=tick.quote,
                close=tick.quote,
                timestamp=candle_start,
            )

            return completed_candle

        # Tick belongs to the current candle
        self.current_candle.high = max(
            self.current_candle.high,
            tick.quote,
        )

        self.current_candle.low = min(
            self.current_candle.low,
            tick.quote,
        )

        self.current_candle.close = tick.quote

        return None


    def load_historical_candles(self, candles: list[Candle]):
        self.candles = candles

        if candles:
            self.current_candle = candles[-1]

    def initialize(self, candles: list[Candle]):
        self.candles = candles
       
        if candles:
            self.current_candle = candles[-1]