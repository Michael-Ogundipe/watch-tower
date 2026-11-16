"""Explainable, as-of-time setup quality and displacement metrics."""

from __future__ import annotations

from dataclasses import dataclass

from models import BreakDirection, FVGStatus, FVGType, Timeframe
from smc_refinement import event_time, swing_confirmation_time


@dataclass(frozen=True)
class DisplacementAssessment:
    category: str
    score: int
    body_expansion: float
    range_expansion: float
    close_strength: float
    meaningful_fvg: bool

    def as_dict(self) -> dict:
        return {
            "category": self.category,
            "score": self.score,
            "body_expansion": round(self.body_expansion, 3),
            "range_expansion": round(self.range_expansion, 3),
            "close_strength": round(self.close_strength, 3),
            "meaningful_fvg": self.meaningful_fvg,
        }


def assess_displacement(break_event, candles, fvg=None, lookback: int = 20) -> DisplacementAssessment:
    """Compare the closed break candle with only candles completed before it."""
    candle = break_event.candle
    previous = [item for item in candles if item.timestamp < candle.timestamp][-lookback:]
    body = abs(candle.close - candle.open)
    span = max(0.0, candle.high - candle.low)
    avg_body = sum(abs(item.close - item.open) for item in previous) / len(previous) if previous else 0.0
    avg_range = sum(max(0.0, item.high - item.low) for item in previous) / len(previous) if previous else 0.0
    body_expansion = body / avg_body if avg_body else 0.0
    range_expansion = span / avg_range if avg_range else 0.0
    if span:
        close_strength = (
            (candle.close - candle.low) / span
            if break_event.direction == BreakDirection.BULLISH
            else (candle.high - candle.close) / span
        )
    else:
        close_strength = 0.0
    meaningful_fvg = bool(
        fvg and fvg.status != FVGStatus.FILLED
        and fvg.fvg_type == (FVGType.BULLISH if break_event.direction == BreakDirection.BULLISH else FVGType.BEARISH)
        and event_time(fvg.candles[-1]) <= event_time(candle)
        and avg_range > 0
        and fvg.upper - fvg.lower >= avg_range * 0.1
    )
    points = int(body_expansion >= 1.25) + int(range_expansion >= 1.2) + int(close_strength >= 0.7) + int(meaningful_fvg)
    category = "strong" if points >= 3 else "moderate" if points == 2 else "weak"
    return DisplacementAssessment(category, points * 25, body_expansion, range_expansion, close_strength, meaningful_fvg)


def premium_discount(direction, structures, current_time) -> dict:
    """Classify against the latest confirmed HTF swing range available as-of time."""
    candidates = []
    for timeframe in (Timeframe.H4, Timeframe.H1):
        structure = structures.get(timeframe)
        if not structure:
            continue
        candles = getattr(structure, "candles", ())
        highs = [s for s in getattr(structure, "swing_highs", ()) if swing_confirmation_time(s, candles) <= current_time]
        lows = [s for s in getattr(structure, "swing_lows", ()) if swing_confirmation_time(s, candles) <= current_time]
        if highs and lows:
            high, low = highs[-1].candle.high, lows[-1].candle.low
            if high > low:
                candidates.append((timeframe, high, low))
    if not candidates:
        return {"state": "unavailable", "equilibrium": None, "timeframe": None, "adjustment": 0}
    timeframe, high, low = candidates[0]  # H4 context preferred, then H1.
    equilibrium = (high + low) / 2
    # Current decision price is the latest closed candle in that HTF structure.
    candles = getattr(structures[timeframe], "candles", ())
    visible = [c for c in candles if event_time(c) <= current_time]
    if not visible:
        state = "unavailable"
    else:
        price = visible[-1].close
        if abs(price - equilibrium) <= 1e-9:
            state = "equilibrium"
        elif price < equilibrium:
            state = "discount"
        else:
            state = "premium"
    favorable = (state == "discount" and direction == BreakDirection.BULLISH) or (state == "premium" and direction == BreakDirection.BEARISH)
    adverse = (state == "premium" and direction == BreakDirection.BULLISH) or (state == "discount" and direction == BreakDirection.BEARISH)
    return {"state": state, "equilibrium": equilibrium, "high": high, "low": low,
            "timeframe": timeframe.name, "adjustment": 8 if favorable else -8 if adverse else 0}


def score_setup_quality(*, direction, htf_poi, sweep, break_event, fvg, order_block,
                        structures, current_time, target_info, displacement, premium_discount_state) -> dict:
    """Return fixed-weight score plus a complete, human-readable breakdown."""
    pool_points = len(getattr(getattr(sweep, "pool", None), "swing_points", ()))
    po = htf_poi["poi"]
    poi_quality = 12 if htf_poi["type"] == "order_block" else 10
    poi_quality = min(12, poi_quality + (2 if len(getattr(po, "candles", ())) >= 3 else 0))
    sweep_candle = getattr(sweep, "candle", None)
    sweep_range = max(0.0, sweep_candle.high - sweep_candle.low) if sweep_candle else 0.0
    reclaim = (((sweep_candle.close - sweep_candle.low) / sweep_range
                if direction == BreakDirection.BULLISH else
                (sweep_candle.high - sweep_candle.close) / sweep_range)
               if sweep_range else 0.0)
    sweep_quality = min(10, 7 + min(2, max(0, pool_points - 1)) + int(reclaim >= 0.7))
    bos_quality = 10 if break_event.break_type.value == "BOS" else 8
    fvg_quality = 8 if fvg else 0
    target_quality = min(7, max(0, int(target_info.get("quality_score", 0) / 7)))
    rr = float(target_info.get("potential_rr") or 0)
    rr_points = min(10, max(0, int(rr * 3)))
    # Entry location is measured inside the mitigated HTF POI, not inferred from future fills.
    ltf_candles = getattr(structures[Timeframe.M5], "candles", ())
    visible = [c for c in ltf_candles if event_time(c) <= current_time]
    location = 5
    if visible and htf_poi["upper"] > htf_poi["lower"]:
        position = (visible[-1].close - htf_poi["lower"]) / (htf_poi["upper"] - htf_poi["lower"])
        if (direction == BreakDirection.BULLISH and position <= 0.4) or (direction == BreakDirection.BEARISH and position >= 0.6):
            location = 10
        elif (direction == BreakDirection.BULLISH and position >= 0.6) or (direction == BreakDirection.BEARISH and position <= 0.4):
            location = 2
    components = {
        "htf_alignment": 12,
        "poi_quality": poi_quality,
        "sweep_quality": sweep_quality,
        "bos_quality": bos_quality,
        "displacement": round(displacement.score * 0.15),
        "fvg_quality": fvg_quality,
        "entry_location": location,
        "premium_discount": int(premium_discount_state["adjustment"] * 0.75),
        "target_quality": target_quality,
        "potential_rr": rr_points,
    }
    score = min(100, max(0, sum(components.values())))
    category = "A+" if score >= 90 else "A" if score >= 80 else "B" if score >= 65 else "C"
    return {"score": score, "category": category, "components": components,
            "displacement": displacement.as_dict(), "premium_discount": premium_discount_state,
            "tp_source": target_info.get("source", "NOT_AVAILABLE"),
            "structural_target": target_info.get("level"), "potential_rr": rr,
            "target_assessment": target_info}
