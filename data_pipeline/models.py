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
    pip_size: int | None


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

class LiquidityType(Enum):
    BUY_SIDE = "buy_side"
    SELL_SIDE = "sell_side"


@dataclass
class LiquidityPool:
    timeframe: Timeframe
    liquidity_type: LiquidityType
    price: float
    swing_points: list[SwingPoint]


@dataclass
class LiquiditySweep:
    timeframe: Timeframe
    liquidity_type: LiquidityType
    pool: LiquidityPool
    candle: Candle


class FVGType(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class FVGStatus(Enum):
    ACTIVE = "active"
    MITIGATED = "mitigated"
    FILLED = "filled"


@dataclass
class FairValueGap:
    timeframe: Timeframe
    fvg_type: FVGType
    upper: float
    lower: float
    candles: list[Candle]
    status: FVGStatus = FVGStatus.ACTIVE


class OrderBlockType(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class OrderBlockStatus(Enum):
    ACTIVE = "active"
    MITIGATED = "mitigated"
    INVALIDATED = "invalidated"

    
@dataclass
class OrderBlock:
    timeframe: Timeframe
    block_type: OrderBlockType
    candle: Candle
    upper: float
    lower: float
    bos: BreakOfStructure
    status: OrderBlockStatus = OrderBlockStatus.ACTIVE
