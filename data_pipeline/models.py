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


@dataclass
class StructurePoint:
    swing: SwingPoint
    structure_type: StructureType


class BreakDirection(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class BreakType(Enum):
    BOS = "bos"
    CHOCH = "choch"


@dataclass
class BreakOfStructure:
    timeframe: Timeframe
    direction: BreakDirection
    break_type: BreakType
    broken_swing: SwingPoint
    candle: Candle


class StructureBias(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass
class MarketStructureState:
    timeframe: Timeframe
    bias: StructureBias
    latest_high: SwingPoint | None
    latest_low: SwingPoint | None
    structure_points: list[StructurePoint]

