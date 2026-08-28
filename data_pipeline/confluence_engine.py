from models import *


class ConfluenceEngine:

    def __init__(
        self,
        structures,
        fvg_detectors,
        order_block_detectors,
        liquidity_sweep_detectors,
        bos_detectors,
    ):
        self.structures = structures
        self.fvg_detectors = fvg_detectors
        self.order_block_detectors = order_block_detectors
        self.liquidity_sweep_detectors = liquidity_sweep_detectors
        self.bos_detectors = bos_detectors

        self.last_signal = None

    def analyze(self):

        score = 0
        reasons = []
        direction = None

        # --------------------------------
        # 1. HTF BIAS
        # --------------------------------

        h1_state = self.structures[Timeframe.H1].get_state()

        if h1_state.bias == StructureBias.BULLISH:
            direction = BreakDirection.BULLISH
            score += 1
            reasons.append("H1 bullish bias")

        elif h1_state.bias == StructureBias.BEARISH:
            direction = BreakDirection.BEARISH
            score += 1
            reasons.append("H1 bearish bias")

        else:
            return None

        # --------------------------------
        # 2. LIQUIDITY SWEEP
        # --------------------------------

        sweep_detector = (
            self.liquidity_sweep_detectors[Timeframe.M5]
        )

        sweep = sweep_detector.get_latest_sweep()

        if sweep:

            if (
                direction == BreakDirection.BULLISH
                and sweep.liquidity_type == LiquidityType.SELL_SIDE
            ):
                score += 2
                reasons.append("Sell-side liquidity swept")

            elif (
                direction == BreakDirection.BEARISH
                and sweep.liquidity_type == LiquidityType.BUY_SIDE
            ):
                score += 2
                reasons.append("Buy-side liquidity swept")

        # --------------------------------
        # 3. BOS / CHOCH
        # --------------------------------

        bos_detector = self.bos_detectors[Timeframe.M15]

        bos = bos_detector.get_latest_break()

        if bos:

            if (
                direction == BreakDirection.BULLISH
                and bos.direction == BreakDirection.BULLISH
            ):
                score += 2
                reasons.append(
                    f"M15 {bos.break_type.value.upper()} bullish"
                )

            elif (
                direction == BreakDirection.BEARISH
                and bos.direction == BreakDirection.BEARISH
            ):
                score += 2
                reasons.append(
                    f"M15 {bos.break_type.value.upper()} bearish"
                )

        # --------------------------------
        # 4. M5 FVG
        # --------------------------------

        fvg_detector = self.fvg_detectors[Timeframe.M5]

        active_fvgs = fvg_detector.get_active_fvgs()

        matching_fvg = next(
            (
                fvg
                for fvg in active_fvgs
                if (
                    fvg.status == FVGStatus.ACTIVE
                    and (
                        (
                            direction == BreakDirection.BULLISH
                            and fvg.fvg_type == FVGType.BULLISH
                        )
                        or (
                            direction == BreakDirection.BEARISH
                            and fvg.fvg_type == FVGType.BEARISH
                        )
                    )
                )
            ),
            None,
        )

        if matching_fvg:
            score += 1
            reasons.append("M5 FVG")

        # --------------------------------
        # 5. M5 ORDER BLOCK
        # --------------------------------

        ob_detector = (
            self.order_block_detectors[Timeframe.M5]
        )

        active_order_blocks = (
            ob_detector.get_active_order_blocks()
        )

        matching_ob = next(
            (
                ob
                for ob in active_order_blocks
                if (
                    ob.status == OrderBlockStatus.ACTIVE
                    and (
                        (
                            direction == BreakDirection.BULLISH
                            and ob.block_type == OrderBlockType.BULLISH
                        )
                        or (
                            direction == BreakDirection.BEARISH
                            and ob.block_type == OrderBlockType.BEARISH
                        )
                    )
                )
            ),
            None,
        )

        if matching_ob:
            score += 1
            reasons.append("M5 Order Block")

        # --------------------------------
        # MINIMUM SETUP REQUIREMENT
        # --------------------------------

        has_liquidity = any(
            "liquidity swept" in reason.lower()
            for reason in reasons
        )

        has_structure = any(
            "m15 bos" in reason.lower()
            or "m15 choch" in reason.lower()
            for reason in reasons
        )

        if not has_liquidity or not has_structure:
            return None

        if score < 4:
            return None

        signal = {
            "direction": direction,
            "score": score,
            "reasons": reasons,
            "fvg": matching_fvg,
            "order_block": matching_ob,
        }

        signal["setup_id"] = (
            direction,
            matching_ob.candle.timestamp
            if matching_ob
            else None,
        )

        return signal