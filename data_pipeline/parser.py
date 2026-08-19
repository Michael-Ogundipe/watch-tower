from datetime import datetime, timezone
from models import Candle, Tick, Timeframe


    

def parse_datetime(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc)


def parse_tick(data: dict) -> Tick:
    tick = data["tick"]

    return Tick(
        symbol=tick["symbol"],
        quote=tick["quote"],
        bid=tick["bid"],
        ask=tick["ask"],
        timestamp=parse_datetime(tick["epoch"]),
        pip_size=tick["pip_size"],
    )

def parse_candles(
    data: dict,
    symbol: str,
    timeframe: Timeframe,
) -> list[Candle]:
    candles = data["candles"]

    return [
        Candle(
            symbol=symbol,
            timeframe=timeframe,
            open=candle["open"],
            high=candle["high"],
            low=candle["low"],
            close=candle["close"],
            timestamp=parse_datetime(candle["epoch"]),
        )
        for candle in candles
    ]

