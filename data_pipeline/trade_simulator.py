from datetime import datetime

from models import Candle, BreakDirection, Timeframe
from trade import Trade, TradeStatus


class TradeSimulator:
    def __init__(self):
        self.pending_trade: Trade | None = None
        self.open_trade: Trade | None = None
        self.completed_trades: list[Trade] = []
        self.pending_candles = 0
        self.max_pending_candles = 60

    def create_trade(self, signal, timestamp: datetime):

        if (
            self.pending_trade is not None
            or self.open_trade is not None
        ):
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

        self.pending_trade = trade
        self.pending_candles = 0

        print(
            f"TRADE PENDING "
            f"{direction.value.upper()} "
            f"ENTRY {entry} "
            f"SL {stop_loss} "
            f"TP {take_profit}"
        )

        return trade

    def process_candle(self, candle: Candle):

        # -----------------------------
        # 1. Check pending entry
        # -----------------------------

        if self.pending_trade is not None:

            trade = self.pending_trade

            # Only M1 candles count toward
            # pending-entry timeout/execution.
            if candle.timeframe == Timeframe.M1:

                self.pending_candles += 1

                # Check whether price reached entry.
                if candle.low <= trade.entry <= candle.high:

                    trade.status = TradeStatus.OPEN
                    trade.entry_time = candle.timestamp

                    self.open_trade = trade
                    self.pending_trade = None
                    self.pending_candles = 0

                    print(
                        f"TRADE FILLED "
                        f"{trade.direction.value.upper()} "
                        f"at {trade.entry} "
                        f"on {candle.timestamp}"
                    )

                # Cancel stale pending setup.
                elif (
                    self.pending_candles
                    >= self.max_pending_candles
                ):

                    print(
                        f"TRADE CANCELLED "
                        f"{trade.direction.value.upper()} "
                        f"ENTRY {trade.entry} "
                        f"reason=PENDING_TIMEOUT"
                    )

                    self.pending_trade = None
                    self.pending_candles = 0

                    return

        # -----------------------------
        # 2. Manage open trade
        # -----------------------------

        if self.open_trade is None:
            return

        # Only M1 candles manage execution.
        if candle.timeframe != Timeframe.M1:
            return

        trade = self.open_trade

        if trade.direction == BreakDirection.BULLISH:

            if candle.low <= trade.stop_loss:
                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            elif candle.high >= trade.take_profit:
                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        elif trade.direction == BreakDirection.BEARISH:

            if candle.high >= trade.stop_loss:
                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            elif candle.low <= trade.take_profit:
                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        if trade.status != TradeStatus.OPEN:

            self.completed_trades.append(trade)

            print(
                f"TRADE CLOSED "
                f"{trade.status.value.upper()} "
                f"exit={trade.exit} "
                f"time={trade.exit_time}"
            )

            self.open_trade = None