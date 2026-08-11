"""
Deriv data provider.

Two responsibilities kept deliberately separate:

1. SymbolResolver — turns a friendly name ("EUR/USD", "Jump 75") into the
   short code Deriv's API actually wants ("frxEURUSD", whatever Jump 75's
   real code is). We resolve this live against Deriv's own `active_symbols`
   call rather than hardcoding a guessed mapping table — Deriv's synthetic
   index codes aren't reliably documented, and a wrong hardcoded code fails
   silently by fetching the wrong instrument. This asks Deriv directly.

2. DerivDataProvider — implements the DataProvider interface using that
   resolver plus a `ticks_history` call with style="candles".
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from deriv_api import DerivAPI

from data_provider import DataProvider
from models import Candle, Timeframe

# Deriv candle granularity is in seconds.
_GRANULARITY_SECONDS: dict[Timeframe, int] = {
    "H4": 14400,
    "H1": 3600,
    "M15": 900,
    "M5": 300,
    "M1": 60,
}


class SymbolResolver:
    """Caches Deriv's active_symbols list in memory and resolves friendly
    names against it. One network round-trip per process, not per call."""

    def __init__(self, api: DerivAPI):
        self._api = api
        self._cache: dict[str, str] | None = None  # normalized display name -> code

    @staticmethod
    def _normalize(name: str) -> str:
        return name.strip().lower().replace("_", " ").replace("-", " ")

    async def _load(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache

        response = await self._api.active_symbols({"active_symbols": "brief"})
        symbols = response.get("active_symbols", [])

        mapping: dict[str, str] = {}
        for entry in symbols:
            # Field names differ across Deriv API versions — handle both.
            code = entry.get("symbol") or entry.get("underlying_symbol")
            display = entry.get("display_name") or entry.get("underlying_symbol_name")
            if code and display:
                mapping[self._normalize(display)] = code

        self._cache = mapping
        return mapping

    async def resolve(self, friendly_name: str) -> str:
        mapping = await self._load()
        needle = self._normalize(friendly_name)

        # Exact match first (handles "EUR/USD" -> "eur/usd" cleanly).
        if needle in mapping:
            return mapping[needle]

        # Fall back to substring match for things like "Jump 75" matching
        # a display name of "Jump 75 Index".
        candidates = [code for display, code in mapping.items() if needle in display]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise ValueError(
                f"'{friendly_name}' matched multiple symbols, be more specific: {candidates}"
            )

        raise ValueError(
            f"No Deriv symbol found matching '{friendly_name}'. "
            f"Check the exact name shown in your Deriv app."
        )


class DerivDataProvider(DataProvider):
    def __init__(self, app_id: str | None = None, api_token: str | None = None):
        self.app_id = app_id or os.environ.get("DERIV_APP_ID")
        self.api_token = api_token or os.environ.get("DERIV_API_TOKEN")  # optional for market data
        if not self.app_id:
            raise ValueError("No Deriv app_id found. Set DERIV_APP_ID in your .env.")

        self._api: DerivAPI | None = None
        self._resolver: SymbolResolver | None = None

    async def _ensure_connected(self) -> None:
        if self._api is None:
            self._api = DerivAPI(app_id=self.app_id)
            self._resolver = SymbolResolver(self._api)

    async def fetch_candles(self, symbol: str, timeframe: Timeframe, count: int) -> list[Candle]:
        await self._ensure_connected()
        code = await self._resolver.resolve(symbol)

        response = await self._api.ticks_history(
            {
                "ticks_history": code,
                "end": "latest",
                "count": count,
                "style": "candles",
                "granularity": _GRANULARITY_SECONDS[timeframe],
                "adjust_start_time": 1,
            }
        )

        raw_candles = response.get("candles", [])
        return [
            Candle(
                timestamp=datetime.fromtimestamp(c["epoch"], tz=timezone.utc),
                open=float(c["open"]),
                high=float(c["high"]),
                low=float(c["low"]),
                close=float(c["close"]),
            )
            for c in raw_candles
        ]

    async def close(self) -> None:
        if self._api is not None:
            await self._api.clear()
