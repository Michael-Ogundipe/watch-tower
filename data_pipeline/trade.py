from dataclasses import dataclass
from enum import Enum

from models import BreakDirection


class TradeStatus(Enum):
    OPEN = "open"
    WIN = "win"
    LOSS = "loss"


@dataclass
class Trade:
    direction: BreakDirection

    entry: float
    stop_loss: float
    take_profit: float

    entry_time: object

    status: TradeStatus = TradeStatus.OPEN
    exit: float | None = None
    exit_time: object | None = None