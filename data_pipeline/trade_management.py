"""Pure, tick-driven protective stop state for open multiplier positions.

Deriv multiplier stop-losses are monetary loss limits, not price stops. The
local protective level below is therefore enforced by closing the contract
when a fresh underlying tick crosses it; the original broker-side monetary
stop remains in place as a fallback.
"""

from __future__ import annotations

from dataclasses import dataclass

from models import BreakDirection


@dataclass
class ProtectionState:
    direction: BreakDirection
    entry_price: float
    initial_stop_price: float
    initial_spread: float
    opened_at: object
    be_trigger_r: float = 1.5
    breakeven_enabled: bool = True
    trailing_enabled: bool = False
    breakeven_armed: bool = False
    protective_stop_price: float | None = None
    close_requested: bool = False
    last_trailing_candle_at: object | None = None

    @property
    def risk_distance(self) -> float:
        return abs(self.entry_price - self.initial_stop_price)


def update_protection(state: ProtectionState, quote: float, structural_stop: float | None = None) -> str | None:
    """Advance stop state monotonically; return ``close`` on a stop cross."""
    long = state.direction == BreakDirection.BULLISH
    risk = state.risk_distance
    if state.breakeven_enabled and risk > 0 and not state.breakeven_armed:
        favorable = quote - state.entry_price if long else state.entry_price - quote
        if favorable >= risk * state.be_trigger_r:
            buffer = max(0.0, state.initial_spread)
            state.protective_stop_price = (
                state.entry_price + buffer if long else state.entry_price - buffer
            )
            state.breakeven_armed = True
            return "breakeven_armed"

    if state.trailing_enabled and structural_stop is not None:
        current = state.protective_stop_price
        if long and structural_stop > state.initial_stop_price and structural_stop < quote and (current is None or structural_stop > current):
            state.protective_stop_price = structural_stop
            return "trailing_stop_advanced"
        if not long and structural_stop < state.initial_stop_price and structural_stop > quote and (current is None or structural_stop < current):
            state.protective_stop_price = structural_stop
            return "trailing_stop_advanced"

    stop = state.protective_stop_price
    if stop is not None and not state.close_requested:
        if (long and quote <= stop) or (not long and quote >= stop):
            state.close_requested = True
            return "close"
    return None
