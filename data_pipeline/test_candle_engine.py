from datetime import datetime, timezone

from candle_engine import CandleEngine
from models import Tick, Timeframe


engine = CandleEngine(Timeframe.M1)

ticks = [
    Tick(
        symbol="R_75",
        quote=100,
        bid=99,
        ask=101,
        timestamp=datetime(2026, 8, 13, 10, 3, 1, tzinfo=timezone.utc),
        pip_size=4,
    ),
    Tick(
        symbol="R_75",
        quote=105,
        bid=104,
        ask=106,
        timestamp=datetime(2026, 8, 13, 10, 3, 20, tzinfo=timezone.utc),
        pip_size=4,
    ),
    Tick(
        symbol="R_75",
        quote=98,
        bid=97,
        ask=99,
        timestamp=datetime(2026, 8, 13, 10, 3, 40, tzinfo=timezone.utc),
        pip_size=4,
    ),
    Tick(
        symbol="R_75",
        quote=110,
        bid=109,
        ask=111,
        timestamp=datetime(2026, 8, 13, 10, 4, 1, tzinfo=timezone.utc),
        pip_size=4,
    ),
]


for tick in ticks:
    completed_candle = engine.process_tick(tick)

    if completed_candle:
        print("Completed:", completed_candle)

print("Current:", engine.current_candle)