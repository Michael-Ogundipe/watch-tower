from models import Candle, BreakDirection
from trade import Trade, TradeStatus


class TradeSimulator:
    def __init__(self):
        self.open_trade: Trade | None = None
        self.completed_trades: list[Trade] = []

    def open(self, trade: Trade):
        if self.open_trade is not None:
            return

        self.open_trade = trade


    def create_trade(self, signal, timestamp: datetime):
        if self.open_trade is not None:
            return None

        order_block = signal["order_block"]

        if order_block is None:
            return None

        direction = signal["direction"]

        entry = (
            order_block.lower + order_block.upper
        ) / 2

        if direction == BreakDirection.BULLISH:

            stop_loss = order_block.lower

            risk = entry - stop_loss

            if risk <= 0:
                return None

            take_profit = entry + (risk * 2)

        else:

            stop_loss = order_block.upper

            risk = stop_loss - entry

            if risk <= 0:
                return None

            take_profit = entry - (risk * 2)

        trade = Trade(
            direction=direction,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_time=timestamp,
        )

        self.open_trade = trade

        return trade

    

    def process_candle(self, candle: Candle):
        if self.open_trade is None:
            return

        trade = self.open_trade

        if trade.direction == BreakDirection.BULLISH:
            # Stop loss first
            if candle.low <= trade.stop_loss:
                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            # Take profit
            elif candle.high >= trade.take_profit:
                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        elif trade.direction == BreakDirection.BEARISH:
            # Stop loss first
            if candle.high >= trade.stop_loss:
                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            # Take profit
            elif candle.low <= trade.take_profit:
                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        if trade.status != TradeStatus.OPEN:
            self.completed_trades.append(trade)
            self.open_trade = None