from datetime import timedelta

from models import (
    BreakDirection,
    FVGType,
    LiquidityType,
    OrderBlockType,
    StructureBias,
    Timeframe,
)
from smc_refinement import (
    find_mitigated_htf_poi,
    find_post_mitigation_ltf_confirmation,
    rank_structural_targets,
)
from setup_quality import assess_displacement, premium_discount, score_setup_quality


class ConfluenceEngine:

    @staticmethod
    def _event_time(candle):
        return candle.timestamp + timedelta(seconds=candle.timeframe.value)

    def __init__(
        self,
        structures,
        fvg_detectors,
        order_block_detectors,
        liquidity_sweep_detectors,
        bos_detectors,
        liquidity_detectors=None,
    ):
        self.structures = structures
        self.fvg_detectors = fvg_detectors
        self.order_block_detectors = order_block_detectors
        self.liquidity_sweep_detectors = liquidity_sweep_detectors
        self.bos_detectors = bos_detectors
        self.liquidity_detectors = liquidity_detectors or {}
    def analyze(self, current_time=None):
        if current_time is None:
            return None

        score = 0
        reasons = []

        # 1. H1 bias
        # get_bias() returns the same value as the original get_state().bias
        # without allocating/copying the complete structure-point list.
        h1_bias = self.structures[Timeframe.H1].get_bias()

        if h1_bias == StructureBias.BULLISH:
            direction = BreakDirection.BULLISH
            score += 1
            reasons.append("H1 bullish bias")
        elif h1_bias == StructureBias.BEARISH:
            direction = BreakDirection.BEARISH
            score += 1
            reasons.append("H1 bearish bias")
        else:
            return None

        # 2. M5 liquidity sweep must be recent and must occur before
        # the structure event used by the setup.
        #
        # Sweeps are appended chronologically. The original code built a
        # filtered list and then searched it backwards. Walking the source
        # list backwards produces the same matching sweep without allocating
        # a temporary list.
        sweep_detector = self.liquidity_sweep_detectors[Timeframe.M5]
        matching_sweep = None

        for sweep in reversed(sweep_detector.sweeps):
            event_time = self._event_time(sweep.candle)

            if event_time > current_time:
                continue

            if (
                direction == BreakDirection.BULLISH
                and sweep.liquidity_type == LiquidityType.SELL_SIDE
            ) or (
                direction == BreakDirection.BEARISH
                and sweep.liquidity_type == LiquidityType.BUY_SIDE
            ):
                matching_sweep = sweep
                break

        if matching_sweep is None:
            return None

        score += 2
        reasons.append(
            "Sell-side liquidity swept"
            if direction == BreakDirection.BULLISH
            else "Buy-side liquidity swept"
        )

        # 3. M15 BOS/CHOCH must be after the sweep and before the current
        # M5 signal candle.
        sweep_time = self._event_time(matching_sweep.candle)
        bos_detector = self.bos_detectors[Timeframe.M15]
        matching_break = None

        for item in reversed(bos_detector.breaks):
            item_time = self._event_time(item.candle)

            if item_time > current_time:
                continue

            if item_time < sweep_time:
                break

            if item.direction == direction:
                matching_break = item
                break

        if matching_break is None:
            return None

        score += 2
        reasons.append(
            f"M15 {matching_break.break_type.value.upper()} "
            f"{direction.value}"
        )

        # 4. M5 FVG must be created after the structure event.
        # FVGs are appended chronologically, so the reverse scan preserves
        # the original active_fvgs[-1] selection.
        fvg_detector = self.fvg_detectors[Timeframe.M5]
        break_time = self._event_time(matching_break.candle)
        matching_fvg = None

        for fvg in reversed(fvg_detector.fvgs):
            fvg_time = self._event_time(fvg.candles[-1])

            if fvg_time > current_time:
                continue

            if fvg_time < break_time:
                break

            if fvg.status.value == "filled":
                continue

            if (
                direction == BreakDirection.BULLISH
                and fvg.fvg_type == FVGType.BULLISH
            ) or (
                direction == BreakDirection.BEARISH
                and fvg.fvg_type == FVGType.BEARISH
            ):
                matching_fvg = fvg
                break

        if matching_fvg:
            score += 1
            reasons.append("M5 FVG")

        # 5. M5 OB must be created after the matching M15 break and be fresh.
        # Order blocks are also appended chronologically.
        ob_detector = self.order_block_detectors[Timeframe.M5]
        matching_ob = None

        for ob in reversed(ob_detector.order_blocks):
            ob_time = self._event_time(ob.bos.candle)

            if ob_time > current_time:
                continue

            if ob_time < break_time:
                break

            if (
                direction == BreakDirection.BULLISH
                and ob.block_type == OrderBlockType.BULLISH
            ) or (
                direction == BreakDirection.BEARISH
                and ob.block_type == OrderBlockType.BEARISH
            ):
                if ob.status.value != "invalidated":
                    matching_ob = ob
                    break

        if matching_ob:
            score += 1
            reasons.append("M5 Order Block")

        # A trade requires the complete core setup.
        if matching_ob is None:
            return None

        if score < 4:
            return None

        # Preserve every existing confluence requirement, then require a
        # direction-aligned HTF POI mitigation followed by a fresh M5 break.
        # M5 is the lowest configured strategy timeframe; M1 remains an
        # execution/fill timeframe in the backtest simulator.
        # The M5 order block associated with that break is the entry structure;
        # the older M5 block remains a required confluence component only.
        ltf_timeframe = Timeframe.M5
        ltf_candles = self.structures[ltf_timeframe].candles
        htf_poi = find_mitigated_htf_poi(
            direction,
            current_time,
            self.structures,
            self.order_block_detectors,
            self.fvg_detectors,
            ltf_candles,
        )
        if htf_poi is None:
            return None

        ltf_confirmation = find_post_mitigation_ltf_confirmation(
            direction,
            current_time,
            htf_poi["mitigated_at"],
            self.bos_detectors[ltf_timeframe],
            self.order_block_detectors[ltf_timeframe],
            timeframe=ltf_timeframe,
        )
        if ltf_confirmation is None:
            return None
        ltf_break, ltf_order_block = ltf_confirmation
        score += 2
        reasons.append(f"{ltf_break.break_type.value.upper()} {ltf_timeframe.name} after {htf_poi['timeframe'].name} POI mitigation")
        reasons.append(f"{htf_poi['timeframe'].name} POI mitigated")

        potential_entry = (ltf_order_block.lower + ltf_order_block.upper) / 2
        stop_price = ltf_order_block.lower if direction == BreakDirection.BULLISH else ltf_order_block.upper
        target_candidates = rank_structural_targets(
            potential_entry, direction, current_time, self.structures,
            getattr(self, "liquidity_detectors", {}), self.fvg_detectors,
            price_increment=0.0, stop_price=stop_price,
        )
        if target_candidates:
            target_info = target_candidates[0]
        else:
            risk = abs(potential_entry - stop_price)
            target_info = {
                "level": potential_entry + 2 * risk if direction == BreakDirection.BULLISH else potential_entry - 2 * risk,
                "source": "2R_fallback", "potential_rr": 2.0, "quality_score": 5,
                "kind": "fallback", "rank": 5,
            }
        displacement = assess_displacement(
            ltf_break, self.structures[ltf_timeframe].candles,
            fvg=matching_fvg,
        )
        pd_state = premium_discount(direction, self.structures, current_time)
        quality = score_setup_quality(
            direction=direction, htf_poi=htf_poi, sweep=matching_sweep,
            break_event=ltf_break, fvg=matching_fvg, order_block=ltf_order_block,
            structures=self.structures, current_time=current_time,
            target_info=target_info, displacement=displacement,
            premium_discount_state=pd_state,
        )

        setup_id = (
            direction,
            self._event_time(matching_sweep.candle),
            self._event_time(matching_break.candle),
            self._event_time(matching_ob.bos.candle),
            htf_poi["created_at"],
            htf_poi["mitigated_at"],
            self._event_time(ltf_break.candle),
        )

        return {
            "direction": direction,
            "score": score,
            "reasons": reasons,
            "fvg": matching_fvg,
            "order_block": ltf_order_block,
            "m5_order_block": matching_ob,
            "htf_poi": htf_poi,
            "ltf_confirmation": ltf_break,
            "quality": quality,
            "setup_id": setup_id,
        }
