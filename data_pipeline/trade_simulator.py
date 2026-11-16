from datetime import datetime, timedelta

from models import Candle, BreakDirection, Timeframe
from smc_refinement import rank_structural_targets
from trade import Trade, TradeStatus


class TradeSimulator:

    def __init__(self, symbol: str, min_entry_rr: float = 0.0):

        self.symbol = symbol
        self.min_entry_rr = min_entry_rr

        self.pending_trade: Trade | None = None
        self.pending_signal: dict | None = None
        self.open_trade: Trade | None = None

        self.completed_trades: list[Trade] = []

        # ----------------------------------------------------
        # Pending-entry settings
        # ----------------------------------------------------

        self.pending_candles = 0
        self.max_pending_candles = 60

        # ----------------------------------------------------
        # Backtest statistics
        # ----------------------------------------------------

        self.total_signals = 0
        self.total_pending = 0
        self.total_filled = 0

        self.total_wins = 0
        self.total_losses = 0
        self.total_timeouts = 0
        self.total_rejected_min_rr = 0
        self.total_invalidated = 0
        self.invalidation_reasons: dict[str, int] = {}

        self.total_r = 0.0

        self.gross_profit_r = 0.0
        self.gross_loss_r = 0.0

    # ========================================================
    # CREATE TRADE
    # ========================================================

    def create_trade(
        self,
        signal,
        timestamp: datetime,
    ):

        # Only one pending/open trade at a time.
        if (
            self.pending_trade is not None
            or self.open_trade is not None
        ):
            return None

        order_block = signal["order_block"]

        if order_block is None:
            return None

        direction = signal["direction"]

        # ----------------------------------------------------
        # Baseline entry model:
        # midpoint of the order block.
        # ----------------------------------------------------

        entry = (
            order_block.lower
            + order_block.upper
        ) / 2

        # ----------------------------------------------------
        # Bullish trade
        # ----------------------------------------------------

        if direction == BreakDirection.BULLISH:

            stop_loss = order_block.lower

            risk = entry - stop_loss

            if risk <= 0:
                return None

            take_profit = (signal.get("quality", {}).get("structural_target")
                           if isinstance(signal.get("quality", {}).get("structural_target"), (int, float))
                           else entry + (risk * 2))

        # ----------------------------------------------------
        # Bearish trade
        # ----------------------------------------------------

        else:

            stop_loss = order_block.upper

            risk = stop_loss - entry

            if risk <= 0:
                return None

            take_profit = (signal.get("quality", {}).get("structural_target")
                           if isinstance(signal.get("quality", {}).get("structural_target"), (int, float))
                           else entry - (risk * 2))

        potential_rr = abs(take_profit - entry) / risk
        if potential_rr < self.min_entry_rr:
            self.total_rejected_min_rr += 1
            return None

        # ----------------------------------------------------
        # Create pending trade
        # ----------------------------------------------------

        trade = Trade(
            direction=direction,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_time=timestamp,
            quality_score=signal.get("quality", {}).get("score"),
            quality_category=signal.get("quality", {}).get("category"),
            tp_source=signal.get("quality", {}).get("tp_source"),
            potential_rr=potential_rr,
        )

        self.pending_trade = trade
        self.pending_signal = signal
        self.pending_candles = 0

        self.total_pending += 1

        print(
            f"[{self.symbol}] "
            f"TRADE PENDING "
            f"{direction.value.upper()} "
            f"ENTRY {entry} "
            f"SL {stop_loss} "
            f"TP {take_profit}"
            f" QUALITY {trade.quality_category or 'NOT_AVAILABLE'}:{trade.quality_score or 'NOT_AVAILABLE'}"
        )

        return trade

    def invalidate_pending_setup(self, candle, service) -> str | None:
        """Apply structural invalidation to pending replay entries on closed M5 bars."""
        trade = self.pending_trade
        signal = self.pending_signal
        if trade is None or signal is None or candle.timeframe != Timeframe.M5:
            return None
        poi = signal.get("htf_poi", {}).get("poi")
        poi_status = getattr(getattr(poi, "status", None), "value", "")
        reason = "HTF_POI_INVALIDATED" if poi_status == "invalidated" else None
        confirmation = signal.get("ltf_confirmation")
        confirm_time = confirmation.candle.timestamp + timedelta(seconds=confirmation.candle.timeframe.value) if confirmation else None
        if not reason and confirm_time:
            for item in reversed(service.bos_detectors[Timeframe.M5].breaks):
                item_time = item.candle.timestamp + timedelta(seconds=item.candle.timeframe.value)
                if item_time <= confirm_time:
                    break
                if item_time <= candle.timestamp + timedelta(seconds=candle.timeframe.value) and item.direction != trade.direction:
                    reason = "CONTRARY_M5_STRUCTURE_BREAK"
                    break
        if not reason and confirm_time and trade.tp_source != "2R_fallback":
            target_crossed = candle.high >= trade.take_profit if trade.direction == BreakDirection.BULLISH else candle.low <= trade.take_profit
            if target_crossed:
                reason = "STRUCTURAL_TARGET_REACHED_BEFORE_ENTRY"
            else:
                stop = trade.stop_loss
                candidates = rank_structural_targets(
                    trade.entry, trade.direction,
                    candle.timestamp + timedelta(seconds=candle.timeframe.value),
                    service.structures, service.liquidity_detectors,
                    service.fvg_detectors, 0.0, stop_price=stop,
                )
                if not any(item.get("source") == trade.tp_source
                           and abs(item["level"] - trade.take_profit) <= max(abs(trade.take_profit) * 0.00001, 1e-9)
                           for item in candidates):
                    reason = "STRUCTURAL_TARGET_UNAVAILABLE"
        if reason:
            self.total_invalidated += 1
            self.invalidation_reasons[reason] = self.invalidation_reasons.get(reason, 0) + 1
            self.pending_trade = None
            self.pending_signal = None
            self.pending_candles = 0
        return reason

    # ========================================================
    # PROCESS M1 CANDLE
    # ========================================================

    def process_candle(self, candle: Candle):

        # ====================================================
        # 1. CHECK PENDING ENTRY
        # ====================================================

        if self.pending_trade is not None:

            trade = self.pending_trade

            if candle.timeframe == Timeframe.M1:

                self.pending_candles += 1

                # ------------------------------------------------
                # Price reached the planned entry.
                # ------------------------------------------------

                if (
                    candle.low
                    <= trade.entry
                    <= candle.high
                ):

                    trade.status = TradeStatus.OPEN

                    trade.entry_time = (
                        candle.timestamp
                    )

                    self.open_trade = trade
                    self.pending_trade = None
                    self.pending_signal = None
                    self.pending_candles = 0

                    self.total_filled += 1

                    print(
                        f"[{self.symbol}] "
                        f"TRADE FILLED "
                        f"{trade.direction.value.upper()} "
                        f"at {trade.entry} "
                        f"on {candle.timestamp}"
                    )

                # ------------------------------------------------
                # Pending order expired.
                # ------------------------------------------------

                elif (
                    self.pending_candles
                    >= self.max_pending_candles
                ):

                    self.total_timeouts += 1

                    print(
                        f"[{self.symbol}] "
                        f"TRADE CANCELLED "
                        f"{trade.direction.value.upper()} "
                        f"ENTRY {trade.entry} "
                        f"reason=PENDING_TIMEOUT"
                    )

                    self.pending_trade = None
                    self.pending_signal = None
                    self.pending_candles = 0

                    return

        # ====================================================
        # 2. MANAGE OPEN TRADE
        # ====================================================

        if self.open_trade is None:
            return

        if candle.timeframe != Timeframe.M1:
            return

        trade = self.open_trade

        # ====================================================
        # BULLISH
        # ====================================================

        if trade.direction == BreakDirection.BULLISH:

            # Conservative assumption:
            # SL wins if both SL and TP are touched
            # inside the same candle.

            if candle.low <= trade.stop_loss:

                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            elif candle.high >= trade.take_profit:

                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        # ====================================================
        # BEARISH
        # ====================================================

        elif trade.direction == BreakDirection.BEARISH:

            if candle.high >= trade.stop_loss:

                trade.status = TradeStatus.LOSS
                trade.exit = trade.stop_loss
                trade.exit_time = candle.timestamp

            elif candle.low <= trade.take_profit:

                trade.status = TradeStatus.WIN
                trade.exit = trade.take_profit
                trade.exit_time = candle.timestamp

        # ====================================================
        # 3. COMPLETE TRADE
        # ====================================================

        if trade.status == TradeStatus.OPEN:
            return

        if trade.status == TradeStatus.WIN:

            self.total_wins += 1
            risk = abs(trade.entry - trade.stop_loss)
            trade.result_r = abs(trade.exit - trade.entry) / risk if risk else 0.0
            self.total_r += trade.result_r
            self.gross_profit_r += trade.result_r

        elif trade.status == TradeStatus.LOSS:

            self.total_losses += 1
            trade.result_r = -1.0
            self.total_r += trade.result_r
            self.gross_loss_r += abs(trade.result_r)

        self.completed_trades.append(trade)

        print(
            f"[{self.symbol}] "
            f"TRADE CLOSED "
            f"{trade.status.value.upper()} "
            f"exit={trade.exit} "
            f"time={trade.exit_time}"
        )

        self.open_trade = None

    # ========================================================
    # PROFIT FACTOR
    # ========================================================

    @property
    def profit_factor(self) -> float:

        if self.gross_loss_r == 0:

            if self.gross_profit_r > 0:
                return float("inf")

            return 0.0

        return (
            self.gross_profit_r
            / self.gross_loss_r
        )

    # ========================================================
    # SUMMARY
    # ========================================================

    def print_summary(self):

        print(
            f"\n========== "
            f"{self.symbol} BACKTEST RESULTS "
            f"=========="
        )

        print(
            f"Signals:        "
            f"{self.total_signals}"
        )

        print(
            f"Pending:        "
            f"{self.total_pending}"
        )

        print(
            f"Filled:         "
            f"{self.total_filled}"
        )

        print(
            f"Wins:           "
            f"{self.total_wins}"
        )

        print(
            f"Losses:         "
            f"{self.total_losses}"
        )

        print(
            f"Timeouts:       "
            f"{self.total_timeouts}"
        )

        print(
            f"Net R:          "
            f"{self.total_r:.2f}R"
        )

        # ----------------------------------------------------
        # Win rate
        # ----------------------------------------------------

        if self.total_filled > 0:

            win_rate = (
                self.total_wins
                / self.total_filled
                * 100
            )

            print(
                f"Win Rate:       "
                f"{win_rate:.2f}%"
            )

        else:

            print(
                "Win Rate:       N/A"
            )

        # ----------------------------------------------------
        # Profit factor
        # ----------------------------------------------------

        if self.profit_factor == float("inf"):

            print(
                "Profit Factor:  ∞"
            )

        else:

            print(
                f"Profit Factor:  "
                f"{self.profit_factor:.2f}"
            )

        # ----------------------------------------------------
        # Unresolved positions
        #
        # We DON'T force-close these at the end of the test.
        # They remain separate so the statistics aren't
        # artificially changed.
        # ----------------------------------------------------

        if self.pending_trade is not None:

            print(
                "Pending at end: YES"
            )

        else:

            print(
                "Pending at end: NO"
            )

        if self.open_trade is not None:

            print(
                "Open at end:    YES"
            )

        else:

            print(
                "Open at end:    NO"
            )

        print(
            "==========================================\n"
        )
