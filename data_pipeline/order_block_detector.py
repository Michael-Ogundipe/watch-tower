from models import *


class OrderBlockDetector:

    def __init__(self, timeframe: Timeframe):
        self.timeframe = timeframe
        self.order_blocks = []

    def check(
        self,
        bos: BreakOfStructure,
        candles: list[Candle],
    ):
        if len(candles) < 2:
            return None

        # Current BOS candle is the last candle.
        previous_candles = candles[:-1]

        # Bullish BOS
        if bos.direction == BreakDirection.BULLISH:

            # Find the last bearish candle before the bullish break.
            for candle in reversed(previous_candles):

                if candle.close < candle.open:

                    order_block = OrderBlock(
                        timeframe=self.timeframe,
                        block_type=OrderBlockType.BULLISH,
                        candle=candle,
                        upper=candle.high,
                        lower=candle.low,
                        bos=bos,
                    )

                    self.order_blocks.append(order_block)

                    return order_block

        # Bearish BOS
        if bos.direction == BreakDirection.BEARISH:

            # Find the last bullish candle before the bearish break.
            for candle in reversed(previous_candles):

                if candle.close > candle.open:

                    order_block = OrderBlock(
                        timeframe=self.timeframe,
                        block_type=OrderBlockType.BEARISH,
                        candle=candle,
                        upper=candle.high,
                        lower=candle.low,
                        bos=bos,
                    )

                    self.order_blocks.append(order_block)

                    return order_block

        return None


    def update_status(
        self,
        candle: Candle,
    ):
        updated = []

        for order_block in self.order_blocks:

            if order_block.status == OrderBlockStatus.INVALIDATED:
                continue

            old_status = order_block.status

            if order_block.block_type == OrderBlockType.BULLISH:

                # Price entered the OB
                if candle.low <= order_block.upper:
                    order_block.status = OrderBlockStatus.MITIGATED

                # Price completely broke below the OB
                if candle.close < order_block.lower:
                    order_block.status = OrderBlockStatus.INVALIDATED

            elif order_block.block_type == OrderBlockType.BEARISH:

                # Price entered the OB
                if candle.high >= order_block.lower:
                    order_block.status = OrderBlockStatus.MITIGATED

                # Price completely broke above the OB
                if candle.close > order_block.upper:
                    order_block.status = OrderBlockStatus.INVALIDATED

            if order_block.status != old_status:
                updated.append(order_block)

        return updated


    def get_active_order_blocks(self):
        return [
            order_block
            for order_block in self.order_blocks
            if order_block.status != OrderBlockStatus.INVALIDATED
        ]