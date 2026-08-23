from models import *


class ConfluenceEngine:

    def __init__(
        self,
        structures,
        fvg_detectors,
        order_block_detectors,
    ):
        self.structures = structures
        self.fvg_detectors = fvg_detectors
        self.order_block_detectors = order_block_detectors

    def analyze(self):

        score = 0
        reasons = []
        direction = None

        # --------------------------------
        # HTF BIAS
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
        # FVG
        # --------------------------------

        fvg_detector = self.fvg_detectors[Timeframe.M5]

        active_fvgs = fvg_detector.get_active_fvgs()

        matching_fvg = next(
            (
                fvg
                for fvg in active_fvgs
                if (
                    direction == BreakDirection.BULLISH
                    and fvg.fvg_type == FVGType.BULLISH
                )
                or (
                    direction == BreakDirection.BEARISH
                    and fvg.fvg_type == FVGType.BEARISH
                )
            ),
            None,
        )

        if matching_fvg:
            score += 1
            reasons.append("M5 FVG")

        # --------------------------------
        # ORDER BLOCK
        # --------------------------------

        ob_detector = self.order_block_detectors[Timeframe.M5]

        active_order_blocks = (
            ob_detector.get_active_order_blocks()
        )

        matching_ob = next(
            (
                ob
                for ob in active_order_blocks
                if (
                    direction == BreakDirection.BULLISH
                    and ob.block_type == OrderBlockType.BULLISH
                )
                or (
                    direction == BreakDirection.BEARISH
                    and ob.block_type == OrderBlockType.BEARISH
                )
            ),
            None,
        )

        if matching_ob:
            score += 1
            reasons.append("M5 Order Block")

        return {
            "direction": direction,
            "score": score,
            "reasons": reasons,
            "fvg": matching_fvg,
            "order_block": matching_ob,
        }