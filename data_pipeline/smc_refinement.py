"""Deterministic helpers for HTF context and LTF execution structure."""

from __future__ import annotations

from datetime import timedelta

from models import (
    BreakDirection,
    FVGStatus,
    FVGType,
    OrderBlockStatus,
    OrderBlockType,
    Timeframe,
)


def event_time(candle):
    return candle.timestamp + timedelta(seconds=candle.timeframe.value)


def swing_confirmation_time(swing, candles):
    """A pivot is usable only after the following candle has closed."""
    if not candles:
        return event_time(swing.candle)
    for index, candle in enumerate(candles):
        if candle is swing.candle:
            if index + 1 < len(candles):
                return event_time(candles[index + 1])
            return event_time(candle) + timedelta(seconds=candle.timeframe.value)
    return event_time(swing.candle) + timedelta(seconds=swing.candle.timeframe.value)


def price_intersects(candle, lower: float, upper: float) -> bool:
    return candle.low <= upper and candle.high >= lower


def find_mitigated_htf_poi(
    direction: BreakDirection,
    current_time,
    structures,
    order_block_detectors,
    fvg_detectors,
    ltf_candles,
):
    """Return the newest known, direction-aligned HTF POI mitigated by LTF.

    Only POIs and closed LTF candles whose completion is no later than
    ``current_time`` are considered. A mitigation time is the first closed
    LTF candle intersecting a POI after its creation.
    """
    expected_ob = (
        OrderBlockType.BULLISH
        if direction == BreakDirection.BULLISH
        else OrderBlockType.BEARISH
    )
    expected_fvg = (
        FVGType.BULLISH
        if direction == BreakDirection.BULLISH
        else FVGType.BEARISH
    )
    candidates = []
    for timeframe in (Timeframe.H4, Timeframe.H1):
        ob_detector = order_block_detectors.get(timeframe)
        for ob in getattr(ob_detector, "order_blocks", ()):
            created = event_time(ob.bos.candle)
            if ob.block_type == expected_ob and ob.status != OrderBlockStatus.INVALIDATED and created <= current_time:
                candidates.append((created, "order_block", ob, ob.lower, ob.upper, timeframe))
        fvg_detector = fvg_detectors.get(timeframe)
        for fvg in getattr(fvg_detector, "fvgs", ()):
            created = event_time(fvg.candles[-1])
            if fvg.fvg_type == expected_fvg and fvg.status != FVGStatus.FILLED and created <= current_time:
                candidates.append((created, "fvg", fvg, fvg.lower, fvg.upper, timeframe))

    mitigations = []
    for created, poi_type, poi, lower, upper, timeframe in candidates:
        touched = next(
            (
        candle for candle in ltf_candles
                if event_time(candle) <= current_time
                and event_time(candle) > created
                and price_intersects(candle, lower, upper)
            ),
            None,
        )
        if touched is not None:
            mitigations.append((created, event_time(touched), poi_type, poi, lower, upper, timeframe))
    if not mitigations:
        return None
    # Prefer the most recently created mitigated POI; ties are stable by
    # timeframe iteration (H4 before H1) and detector insertion order.
    created, mitigated_at, poi_type, poi, lower, upper, timeframe = max(
        mitigations, key=lambda item: item[0]
    )
    return {
        "created_at": created,
        "mitigated_at": mitigated_at,
        "type": poi_type,
        "poi": poi,
        "lower": lower,
        "upper": upper,
        "timeframe": timeframe,
    }


def find_post_mitigation_ltf_confirmation(
    direction: BreakDirection,
    current_time,
    mitigation_time,
    bos_detector,
    order_block_detector,
    timeframe: Timeframe = Timeframe.M5,
):
    """Find a directional strategy-LTF BOS/CHoCH strictly after POI touch."""
    matches = []
    for item in bos_detector.breaks:
        timestamp = event_time(item.candle)
        if (
            item.direction == direction
            and item.timeframe == timeframe
            and timestamp > mitigation_time
            and timestamp <= current_time
        ):
            matches.append(item)
    for confirmed_break in reversed(matches):
        for ob in reversed(order_block_detector.order_blocks):
            if ob.bos is confirmed_break and ob.status != OrderBlockStatus.INVALIDATED:
                return confirmed_break, ob
    return None


def select_structural_target(
    entry: float,
    direction: BreakDirection,
    current_time,
    structures,
    liquidity_detectors,
    fvg_detectors,
    price_increment: float,
) -> float | None:
    """Compatibility wrapper returning the top ranked target level."""
    ranked = rank_structural_targets(
        entry, direction, current_time, structures, liquidity_detectors,
        fvg_detectors, price_increment, stop_price=entry,
    )
    return ranked[0]["level"] if ranked else None


def rank_structural_targets(
    entry: float,
    direction: BreakDirection,
    current_time,
    structures,
    liquidity_detectors,
    fvg_detectors,
    price_increment: float,
    *,
    stop_price: float | None = None,
) -> list[dict]:
    """Rank known opposing targets; swing candidates require confirmation."""
    candidates: list[dict] = []
    for timeframe, structure in structures.items():
        # Price targets use established M15/H1/H4 structure; M1 pivots are
        # entry precision and are deliberately not treated as major targets.
        if timeframe in (Timeframe.M15, Timeframe.H1, Timeframe.H4):
            structure_candles = getattr(structure, "candles", ())
            for swing in getattr(structure, "swing_highs" if direction == BreakDirection.BULLISH else "swing_lows", ()):
                level = swing.candle.high if direction == BreakDirection.BULLISH else swing.candle.low
                confirmed_at = swing_confirmation_time(swing, structure_candles)
                if confirmed_at <= current_time and (
                    level > entry if direction == BreakDirection.BULLISH else level < entry
                ):
                    tested = any(
                        event_time(candle) > confirmed_at and event_time(candle) <= current_time
                        and (candle.high >= level if direction == BreakDirection.BULLISH else candle.low <= level)
                        for candle in structure_candles
                    )
                    candidates.append({"raw_level": level, "kind": "swing", "timeframe": timeframe,
                                       "external": timeframe in (Timeframe.H1, Timeframe.H4),
                                       "significance": 2 if timeframe == Timeframe.H4 else 1,
                                       "tested": tested})
        detector = liquidity_detectors.get(timeframe)
        if timeframe not in (Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4):
            detector = None
        for pool in getattr(detector, "pools", ()):
            side_matches = pool.liquidity_type.value == (
                "buy_side" if direction == BreakDirection.BULLISH else "sell_side"
            )
            visible_points = [
                point for point in pool.swing_points
                if swing_confirmation_time(point, getattr(structure, "candles", ())) <= current_time
            ]
            if side_matches and len(visible_points) >= 2:
                values = [
                    point.candle.high if direction == BreakDirection.BULLISH else point.candle.low
                    for point in visible_points
                ]
                visible_level = sum(values) / len(values)
                if visible_level > entry if direction == BreakDirection.BULLISH else visible_level < entry:
                    latest_point_confirmation = max(
                        swing_confirmation_time(point, getattr(structure, "candles", ()))
                        for point in visible_points
                    )
                    tested = any(
                        event_time(candle) > latest_point_confirmation and event_time(candle) <= current_time
                        and (candle.high >= visible_level if direction == BreakDirection.BULLISH else candle.low <= visible_level)
                        for candle in getattr(structure, "candles", ())
                    )
                    candidates.append({"raw_level": visible_level, "kind": "equal_liquidity", "timeframe": timeframe,
                                       "external": timeframe in (Timeframe.H1, Timeframe.H4),
                                       "significance": 3, "tested": tested})

    for timeframe, detector in fvg_detectors.items():
        if timeframe not in (Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4):
            continue
        for fvg in getattr(detector, "fvgs", ()):
            if fvg.status != FVGStatus.ACTIVE or event_time(fvg.candles[-1]) > current_time:
                continue
            if direction == BreakDirection.BULLISH and fvg.fvg_type == FVGType.BEARISH and fvg.lower > entry:
                candidates.append({"raw_level": fvg.lower, "kind": "opposing_fvg", "timeframe": timeframe,
                                   "external": timeframe in (Timeframe.H1, Timeframe.H4), "significance": 1, "tested": False})
            elif direction == BreakDirection.BEARISH and fvg.fvg_type == FVGType.BULLISH and fvg.upper < entry:
                candidates.append({"raw_level": fvg.upper, "kind": "opposing_fvg", "timeframe": timeframe,
                                   "external": timeframe in (Timeframe.H1, Timeframe.H4), "significance": 1, "tested": False})

    buffer = max(0.0, price_increment)
    risk = abs(entry - stop_price) if stop_price is not None else 0.0
    ranked = []
    for candidate in candidates:
        level = candidate["raw_level"]
        target = level - buffer if direction == BreakDirection.BULLISH else level + buffer
        reward = target - entry if direction == BreakDirection.BULLISH else entry - target
        if reward <= 0:
            continue
        rr = reward / risk if risk > 0 else 0.0
        # Progressively discount remote levels; do not impose a fixed RR ceiling.
        reachability = max(0, 15 - int(max(0.0, rr - 2.0) * 3))
        quality = (20 if candidate["external"] else 0) + candidate["significance"] * 5
        quality += 12 if candidate["kind"] == "equal_liquidity" else 8 if candidate["kind"] == "opposing_fvg" else 0
        quality += reachability - (10 if candidate["tested"] else 0)
        candidate.update({"level": target, "potential_rr": rr, "quality_score": quality,
                          "source": candidate["kind"], "reachability_score": reachability})
        # Prefer meaningful external pools, then stronger RR/reachability; distance is a tie breaker.
        candidate["rank"] = quality + min(20, rr * 2)
        ranked.append(candidate)
    return sorted(ranked, key=lambda item: (item["rank"], -abs(item["level"] - entry)), reverse=True)
