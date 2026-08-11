"""
Market data providers.

DataProvider is the abstraction the rest of the pipeline depends on.
Swap TwelveDataProvider for an OANDA/broker implementation later without
touching anything downstream — that's the whole point of the interface.
"""

from __future__ import annotations

import asyncio
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import requests

from models import Candle, MarketSnapshot, Timeframe, TimeframeSnapshot

# Maps our timeframe names to each provider's own vocabulary.
_TWELVEDATA_INTERVALS: dict[Timeframe, str] = {
    "H4": "4h",
    "H1": "1h",
    "M15": "15min",
    "M5": "5min",
    "M1": "1min",
}

ALL_TIMEFRAMES: list[Timeframe] = ["H4", "H1", "M15", "M5", "M1"]


class DataProvider(ABC):
    @abstractmethod
    async def fetch_candles(self, symbol: str, timeframe: Timeframe, count: int) -> list[Candle]:
        """Return the most recent `count` candles, oldest first."""
        raise NotImplementedError

    async def _fetch_one(self, symbol: str, tf: Timeframe, count: int) -> TimeframeSnapshot:
        try:
            candles = await self.fetch_candles(symbol, tf, count)
        except Exception as exc:  # noqa: BLE001 - deliberately broad, isolate failures
            print(f"[warn] fetch failed for {symbol} {tf}: {exc}")
            candles = []
        return TimeframeSnapshot(timeframe=tf, candles=candles)

    async def fetch_snapshot(
        self, symbol: str, timeframes: list[Timeframe] = ALL_TIMEFRAMES, count: int = 50
    ) -> MarketSnapshot:
        """Fetch every requested timeframe concurrently. One failure doesn't
        kill the rest — a timeframe that fails just comes back empty and gets
        flagged as no_data downstream, per the pipeline's failure-isolation
        requirement. Concurrency also matters here: five sequential H4-M1
        fetches would add up fast against the <2s latency target."""
        snapshots = await asyncio.gather(
            *(self._fetch_one(symbol, tf, count) for tf in timeframes)
        )
        result = {snap.timeframe: snap for snap in snapshots}

        return MarketSnapshot(
            symbol=symbol,
            fetched_at=datetime.now(timezone.utc),
            timeframes=result,
        )


class TwelveDataProvider(DataProvider):
    BASE_URL = "https://api.twelvedata.com/time_series"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("TWELVEDATA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "No Twelve Data API key found. Set TWELVEDATA_API_KEY in your .env, "
                "or pass api_key= explicitly."
            )

    async def fetch_candles(self, symbol: str, timeframe: Timeframe, count: int) -> list[Candle]:
        interval = _TWELVEDATA_INTERVALS[timeframe]
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": count,
            "apikey": self.api_key,
            "order": "ASC",
        }
        # requests is blocking; run it off the event loop so it doesn't stall
        # the other concurrent timeframe fetches in fetch_snapshot.
        resp = await asyncio.to_thread(requests.get, self.BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "error":
            raise RuntimeError(f"Twelve Data error: {data.get('message')}")

        candles = []
        for row in data.get("values", []):
            candles.append(
                Candle(
                    timestamp=datetime.fromisoformat(row["datetime"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume", 0) or 0),
                )
            )
        return candles


class MockDataProvider(DataProvider):
    """Deterministic fake data for local testing without hitting a real API
    or spending your Twelve Data quota."""

    async def fetch_candles(self, symbol: str, timeframe: Timeframe, count: int) -> list[Candle]:
        import random

        random.seed(f"{symbol}-{timeframe}")
        base = 1.0850
        price = base
        candles = []
        now = datetime.now(timezone.utc)
        for i in range(count):
            drift = random.uniform(-0.0015, 0.0015)
            o = price
            c = price + drift
            h = max(o, c) + random.uniform(0, 0.0008)
            l = min(o, c) - random.uniform(0, 0.0008)
            candles.append(
                Candle(timestamp=now, open=o, high=h, low=l, close=c, volume=random.uniform(100, 1000))
            )
            price = c
        return candles
