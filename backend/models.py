"""
Domain models for WatchTower's data layer.

Kept intentionally small for this stage: a Candle, a TimeframeSnapshot
(one timeframe's recent candles for a symbol), and a MarketSnapshot
(all timeframes for a symbol, bundled for a single analysis pass).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Timeframe = Literal["H4", "H1", "M15", "M5", "M1"]


class Candle(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def is_bullish(self) -> bool:
        return self.close > self.open


class TimeframeSnapshot(BaseModel):
    timeframe: Timeframe
    candles: list[Candle] = Field(default_factory=list)

    def latest(self) -> Candle | None:
        return self.candles[-1] if self.candles else None

    def summary(self) -> dict:
        """Compact stats an LLM can reason over without needing every candle."""
        if not self.candles:
            return {"timeframe": self.timeframe, "status": "no_data"}

        closes = [c.close for c in self.candles]
        highs = [c.high for c in self.candles]
        lows = [c.low for c in self.candles]
        first, last = self.candles[0], self.candles[-1]

        return {
            "timeframe": self.timeframe,
            "candle_count": len(self.candles),
            "range_start": first.timestamp.isoformat(),
            "range_end": last.timestamp.isoformat(),
            "last_close": last.close,
            "period_high": max(highs),
            "period_low": min(lows),
            "net_change": round(last.close - first.close, 5),
            "net_change_pct": round((last.close - first.close) / first.close * 100, 3)
            if first.close
            else None,
            "last_5_closes": closes[-5:],
        }


class MarketSnapshot(BaseModel):
    symbol: str
    fetched_at: datetime
    timeframes: dict[Timeframe, TimeframeSnapshot]

    def to_llm_payload(self) -> dict:
        """Structured, compact JSON for the LLM — no raw candle floods."""
        return {
            "symbol": self.symbol,
            "fetched_at": self.fetched_at.isoformat(),
            "timeframes": {
                tf: snap.summary() for tf, snap in self.timeframes.items()
            },
        }
