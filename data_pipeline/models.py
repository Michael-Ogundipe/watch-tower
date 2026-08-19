from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass
class Tick:
    """Represents a single market tick."""

    symbol: str
    quote: float
    bid: float
    ask: float
    timestamp: datetime
    pip_size: int


class Timeframe(Enum):
    M1 = 60
    M5 = 300
    M15 = 900
    H1 = 3600
    H4 = 14400


@dataclass
class Candle:
    symbol: str
    timeframe: Timeframe
    open: float
    high: float
    low: float
    close: float
    timestamp: datetime


class SwingType(Enum):
    HIGH = "high"
    LOW = "low"

@dataclass
class SwingPoint:
    candle: Candle
    type: SwingType


class StructureType(Enum):
    HH = "higher_high"
    HL = "higher_low"
    LH = "lower_high"
    LL = "lower_low"