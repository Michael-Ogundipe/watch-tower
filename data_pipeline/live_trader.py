"""
Live trading orchestrator for the Deriv DEMO account.

Uses the same MarketDataService, detectors, and ConfluenceEngine
as the backtester.

LIVE DEMO EXECUTION:
- Connects to the explicitly configured DEMO account.
- Watches live ticks for every configured symbol at the same time.
- Builds candles across all configured timeframes, per symbol.
- Detects confluence signals, per symbol.
- Waits for price to enter the order-block zone, per symbol.
- Calculates stake using position_sizing.compute_stake().
- Calls buy_multiplier() to place the actual DEMO trade.
- Tracks the opened contract until it closes.

MULTI-SYMBOL:
- Each symbol is watched by its own LiveTrader instance, with fully
  independent state: its own candles/structure/confluence engine, its
  own pending_signal, and its own open_contract_id.
- One open position at a time PER SYMBOL -- but symbols are otherwise
  independent, so e.g. JD75 can be in a trade while R_75 opens one at
  the same time.
- All LiveTrader instances share ONE authenticated DerivTradingClient
  connection to the DEMO account. That client already multiplexes
  concurrent proposal/buy/contract-subscription requests by request
  id, so this is safe -- it also means a hard reconnect on that shared
  connection (rare; only on real connection loss) can momentarily
  affect an in-flight order on another symbol. Watch Tower already
  treats a failed BUY as "not placed" and resets that symbol's pending
  signal rather than guessing, same as before.

IMPORTANT:
- This file is configured for DEMO execution.
- There is NO dry-run mode.
- config.DEMO_ACCOUNT_ID must point to the Deriv DEMO account.
"""

import asyncio
import json
import math
import os
import time
from decimal import Decimal
from datetime import datetime, timedelta, timezone

from models import BreakDirection, Timeframe
from market_data_service import MarketDataService
from deriv_trading_client import (
    DerivTradingClient,
    ExecutionPriceMovedError,
    MultiplierOutOfRangeError,
)
from confluence_history import (
    record_confluence,
    update_confluence,
)
from position_sizing import compute_stake
from trade_history import get_open_trade_rows, record_trade, update_trade
from trade_observability import (
    NOT_AVAILABLE,
    broker_value,
    contract_created_at,
    contract_updated_at,
)
import db_sink
from ops_observability import emit_event, emit_runtime_status
from smc_refinement import event_time, rank_structural_targets
from trade_management import ProtectionState, update_protection


HEARTBEAT_INTERVAL = timedelta(minutes=15)


class LiveTrader:
    """
    Watches ONE symbol end-to-end and holds at most one open position
    for that symbol at a time.

    `trading_client` must already be connected. It is shared across
    every symbol's LiveTrader when running multiple symbols at once --
    this class never connects or closes it.
    """

    def __init__(
        self,
        symbol: str,
        trading_client: DerivTradingClient,
        multiplier: float = 100,
        risk_amount: float = 5.00,
        min_stake: float = 1.0,
        max_stake: float = 100.0,
        max_pending_ticks: int = 3000,
        max_entry_spread: float = 1.0,
        breakeven_enabled: bool = True,
        breakeven_trigger_r: float = 1.5,
        structural_trailing_enabled: bool = False,
        min_entry_rr: float = 0.0,
    ):
        if max_entry_spread <= 0 or breakeven_trigger_r <= 0:
            raise ValueError("Entry spread and breakeven trigger thresholds must be positive.")
        self.symbol = symbol

        self.market_service = MarketDataService(symbol)

        self.trading_client = trading_client

        self.multiplier = multiplier
        self.risk_amount = risk_amount
        self.min_stake = min_stake
        self.max_stake = max_stake
        self.max_pending_ticks = max_pending_ticks
        self.max_entry_spread = max_entry_spread
        self.breakeven_enabled = breakeven_enabled
        self.breakeven_trigger_r = breakeven_trigger_r
        self.structural_trailing_enabled = structural_trailing_enabled
        if min_entry_rr < 0 or not math.isfinite(min_entry_rr):
            raise ValueError("Minimum entry R:R must be finite and non-negative.")
        self.min_entry_rr = min_entry_rr

        self.pending_signal = None
        self.open_contracts = {}
        self.open_trade_protection: dict[str, ProtectionState] = {}
        self.last_heartbeat = None
        self.last_runtime_publish = 0.0
        self.process_started_at = datetime.now(timezone.utc)
        self.last_processed_candle = None
        self.last_error = None

    @property
    def open_contract_id(self):
        """Return one active ID for the existing heartbeat status field."""
        return next(iter(self.open_contracts), None)

    def _log(self, message: str):
        instance_id = os.environ.get("INSTANCE_ID", "unconfigured")
        print(f"[INSTANCE={instance_id}][{self.symbol}] {message}", flush=True)

    def _publish_runtime_status(self, tick=None, *, force: bool = False, running: bool = True):
        """Queue a passive per-symbol snapshot at a bounded rate."""
        now_mono = time.monotonic()
        if not force and now_mono - self.last_runtime_publish < 10:
            return
        self.last_runtime_publish = now_mono
        trading = self.trading_client
        account = (getattr(trading, "accounts_by_id", {}) or {}).get(getattr(trading, "account_id", None), {})
        pending = self.pending_signal
        active = None
        if pending:
            ob = pending.get("order_block")
            active = {
                "signal_id": pending.get("setup_id"),
                "signal_time": pending.get("signal_time"),
                "direction": getattr(pending.get("direction"), "value", pending.get("direction")),
                "timeframe": getattr(getattr(ob, "timeframe", None), "value", None),
                "score": pending.get("score"),
                "setup_id": pending.get("setup_id"),
                "ob_lower": getattr(ob, "lower", None),
                "ob_upper": getattr(ob, "upper", None),
                "ticks_waited": pending.get("ticks_waited", 0),
            }
        balance_data = getattr(trading, "last_balance", {}) or {}
        market_client = self.market_service.client
        db_status = db_sink.database_write_status()
        tick_at = tick.timestamp.isoformat() if tick is not None else None
        state = {
            "running": running,
            "process_started_at": self.process_started_at.isoformat(),
            "process_uptime_seconds": max(0, int((datetime.now(timezone.utc) - self.process_started_at).total_seconds())),
            "symbol": self.symbol,
            "deriv_authenticated": bool(
                getattr(trading, "authenticated", False)
                and getattr(trading, "_socket_healthy", lambda: False)()
            ),
            "account_id_redacted": getattr(trading, "_masked_account_id", lambda x: None)(getattr(trading, "account_id", None)),
            "account_type": account.get("account_type"),
            "balance": balance_data.get("balance") if isinstance(balance_data, dict) else None,
            "currency": balance_data.get("currency") if isinstance(balance_data, dict) else None,
            "market_data_connected": bool(getattr(market_client, "connected", False)),
            "last_tick_at": tick_at or getattr(self, "last_tick_at", None),
            "last_heartbeat_at": tick_at or getattr(self, "last_tick_at", None),
            "last_candle_at": self.last_processed_candle,
            "reconnect_count": getattr(market_client, "reconnect_count", 0),
            "last_disconnect_at": getattr(market_client, "last_disconnect_at", None),
            "last_reconnect_at": getattr(market_client, "last_reconnect_at", None),
            "pending_signal": active,
            "open_contract_count": len(self.open_contracts),
            "open_contract_ids": list(self.open_contracts.keys()),
            "database_write": db_status,
            "last_error": self.last_error,
        }
        emit_runtime_status(f"trader:{self.symbol}", state)

    def _expire_pending_after_market_data_gap(self) -> bool:
        """Do not let a pending setup survive a feed outage and enter stale."""
        pending = self.pending_signal
        if not pending:
            return False

        disconnected_at = getattr(self.market_service.client, "last_disconnect_at", None)
        signal_time = pending.get("signal_time")
        if not disconnected_at or not signal_time:
            return False

        try:
            disconnect_time = datetime.fromisoformat(disconnected_at)
            created_time = datetime.fromisoformat(signal_time)
        except (TypeError, ValueError):
            return False

        if disconnect_time <= created_time:
            return False

        self.pending_signal = None
        self._log("PENDING SIGNAL EXPIRED after a market-data disconnect.")
        emit_event(
            "SIGNAL_EXPIRED", severity="WARN", component="trader", symbol=self.symbol,
            timeframe=pending.get("timeframe"),
            signal_id=str(pending.get("setup_id", "")) or None,
            message="Pending signal expired because market data disconnected before price entered its zone.",
            reason="MARKET_DATA_DISCONNECTED",
            details={"disconnected_at": disconnected_at},
        )
        self._publish_runtime_status(force=True)
        return True

    def _invalidate_pending_setup(self, candle) -> bool:
        """Invalidate a pending setup on structural thesis failure, never age alone."""
        pending = self.pending_signal
        if not pending or candle.timeframe != Timeframe.M5:
            return False
        poi = pending.get("htf_poi", {}).get("poi")
        status = getattr(getattr(poi, "status", None), "value", "")
        poi_type = pending.get("htf_poi", {}).get("type")
        reason = "HTF_POI_INVALIDATED" if status == "invalidated" else None
        confirmation = pending.get("ltf_confirmation")
        confirmed_at = event_time(confirmation.candle) if confirmation else None
        if not reason and confirmed_at:
            for structure_break in reversed(self.market_service.bos_detectors[Timeframe.M5].breaks):
                event_at = event_time(structure_break.candle)
                if event_at <= confirmed_at:
                    break
                if event_at <= event_time(candle) and structure_break.direction != pending["direction"]:
                    reason = "CONTRARY_M5_STRUCTURE_BREAK"
                    break
        quality = pending.get("quality", {})
        target = quality.get("structural_target")
        source = quality.get("tp_source")
        if not reason and isinstance(target, (int, float)) and source != "2R_fallback" and confirmed_at and event_time(candle) > confirmed_at:
            crossed = candle.high >= target if pending["direction"] == BreakDirection.BULLISH else candle.low <= target
            if crossed:
                reason = "STRUCTURAL_TARGET_REACHED_BEFORE_ENTRY"
            else:
                ob = pending["order_block"]
                entry_estimate = (ob.lower + ob.upper) / 2
                stop = ob.lower if pending["direction"] == BreakDirection.BULLISH else ob.upper
                current_targets = rank_structural_targets(
                    entry_estimate, pending["direction"], event_time(candle),
                    self.market_service.structures, self.market_service.liquidity_detectors,
                    self.market_service.fvg_detectors, 0.0, stop_price=stop,
                )
                tolerance = max(abs(float(target)) * 0.00001, 1e-9)
                if not any(item.get("source") == source and abs(item["level"] - target) <= tolerance
                           for item in current_targets):
                    reason = "STRUCTURAL_TARGET_UNAVAILABLE"
        if not reason:
            return False
        setup_id = str(pending.get("setup_id", "")) or None
        self.pending_signal = None
        row = pending.get("confluence_row")
        if row is not None:
            update_confluence(row, status="INVALIDATED", rejection_reason=reason,
                              setup_invalidation_reason=reason)
        emit_event("SIGNAL_INVALIDATED", severity="WARN", component="trader", symbol=self.symbol,
                   timeframe=Timeframe.M5.value, signal_id=setup_id,
                   message="Pending setup invalidated by structural state.", reason=reason,
                   details={"candle_time": event_time(candle).isoformat()})
        self._publish_runtime_status(force=True)
        return True

    async def start(self):
        self._publish_runtime_status(force=True)
        emit_event("TRADER_STARTING", component="trader", symbol=self.symbol,
                   message="Symbol trader starting.")
        try:
            await self.market_service.initialize()
            await self._resume_open_contracts()
            self._publish_runtime_status(force=True)
            replayed = sum(len(engine.candles) for engine in self.market_service.engines.values())
            emit_event("STARTUP_STATE_REPLAY_COMPLETED", component="market_data", symbol=self.symbol,
                       message="Startup market state reconstruction completed.",
                       details={"closed_candles_loaded": replayed})
            self._log(
                "Watch Tower will place trades on the configured "
                "Deriv DEMO account when a valid setup reaches entry."
            )
            emit_event("TRADER_STARTED", component="trader", symbol=self.symbol,
                       message="Symbol trader initialized and processing market data.")
            await self._run()
        finally:
            self._publish_runtime_status(force=True, running=False)
            emit_event("TRADER_STOPPED", severity="WARN", component="trader", symbol=self.symbol,
                       message="Symbol trader stopped.")

    async def _resume_open_contracts(self):
        """Resume contract subscriptions for open journal rows after restart."""
        for trade in get_open_trade_rows(self.symbol):
            contract_id = str(trade["contract_id"])
            if contract_id in self.open_contracts:
                continue
            row_index = trade["row_index"]
            self.open_contracts[contract_id] = row_index
            try:
                saved_direction = str(trade.get("direction", "")).strip().upper()
                direction = (
                    BreakDirection.BULLISH if saved_direction in {"UP", "BULLISH"}
                    else BreakDirection.BEARISH if saved_direction in {"DOWN", "BEARISH"}
                    else None
                )
                if direction is None:
                    raise ValueError("unsupported saved direction")
                entry_price = float(trade.get("entry_price") or trade.get("entry"))
                stop_price = float(trade.get("technical_sl_price") or trade.get("stop_loss_price"))
                self.open_trade_protection[contract_id] = ProtectionState(
                    direction=direction,
                    entry_price=entry_price,
                    initial_stop_price=stop_price,
                    initial_spread=0.0,
                    opened_at=self.process_started_at,
                    be_trigger_r=self.breakeven_trigger_r,
                    breakeven_enabled=self.breakeven_enabled,
                    trailing_enabled=self.structural_trailing_enabled,
                )
            except (TypeError, ValueError):
                self._log(f"Protective price management unavailable for resumed contract {contract_id}: missing journal levels.")
            self._log(
                f"Resuming persisted open contract {contract_id} "
                f"from trade_history row {row_index}."
            )
            asyncio.create_task(
                self._watch_open_contract(contract_id, row_index)
            )

    def _maybe_print_heartbeat(self, tick):
        if (
            self.last_heartbeat is not None
            and tick.timestamp - self.last_heartbeat
            < HEARTBEAT_INTERVAL
        ):
            return

        self.last_heartbeat = tick.timestamp

        h1_structure = self.market_service.structures.get(
            Timeframe.H1
        )

        bias = (
            h1_structure.get_bias().value
            if h1_structure
            else "unknown"
        )

        self._log(
            f"[{tick.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"heartbeat: "
            f"price={tick.quote} "
            f"H1_bias={bias} "
            f"pending_signal="
            f"{'yes' if self.pending_signal else 'no'} "
            f"open_contract="
            f"{self.open_contract_id or 'no'}"
        )

    def _print_candle_diagnostics(self, completed_candle):
        """
        Diagnostic output only.

        Does not alter strategy or signal logic.
        """

        timeframe = completed_candle.timeframe

        if timeframe not in (
            Timeframe.M5,
            Timeframe.M15,
            Timeframe.H1,
        ):
            return

        h1_structure = self.market_service.structures.get(
            Timeframe.H1
        )

        bias = (
            h1_structure.get_bias().value
            if h1_structure
            else "unknown"
        )

        self._log(
            f"[DIAGNOSTIC] "
            f"TIMEFRAME={timeframe.value} CLOSED "
            f"{completed_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S')} "
            f"price={completed_candle.close} "
            f"H1_bias={bias}"
        )

    async def _run(self):
        async for tick in self.market_service.client.subscribe_ticks(
            self.symbol
        ):
            self._expire_pending_after_market_data_gap()
            self.last_tick_at = tick.timestamp.isoformat()
            self._maybe_print_heartbeat(tick)
            self._publish_runtime_status(tick)
            await self._manage_open_positions(tick)

            # Feed the live tick into every timeframe.
            for engine in self.market_service.engines.values():

                completed_candle = engine.process_tick(tick)

                if completed_candle:
                    self.last_processed_candle = self.market_service.candle_completion_time(completed_candle).isoformat()
                    emit_event(
                        "CANDLE_CLOSED", component="market_data", symbol=self.symbol,
                        timeframe=completed_candle.timeframe.value,
                        message="A live candle was finalized and passed to the existing strategy path.",
                        details={"candle_time": completed_candle.timestamp.isoformat(),
                                 "open": completed_candle.open, "high": completed_candle.high,
                                 "low": completed_candle.low, "close": completed_candle.close},
                    )

                    self._print_candle_diagnostics(
                        completed_candle
                    )

                    signal = (
                        self.market_service.process_completed_candle(
                            completed_candle
                        )
                    )
                    self._invalidate_pending_setup(completed_candle)

                    if signal:
                        emit_event(
                            "CONFLUENCE_FOUND", component="strategy_observer", symbol=self.symbol,
                            timeframe=completed_candle.timeframe.value,
                            signal_id=str(signal.get("setup_id", "")) or None,
                            message="Confluence engine returned a setup.",
                            details={"direction": signal["direction"].value.upper(),
                                     "score": signal.get("score"),
                                     "reasons": signal.get("reasons", []),
                                     "candle_time": completed_candle.timestamp.isoformat()},
                        )
                        self._log(
                            "[DIAGNOSTIC] "
                            "CONFLUENCE FOUND "
                            f"direction="
                            f"{signal['direction'].value.upper()} "
                            f"score="
                            f"{signal.get('score', 'unknown')}"
                        )

                    else:
                        if completed_candle.timeframe == Timeframe.M5:
                            emit_event(
                                "CONFLUENCE_NOT_FOUND", component="strategy_observer", symbol=self.symbol,
                                timeframe=completed_candle.timeframe.value,
                                message="Existing confluence evaluation returned no setup.",
                                details={"candle_time": completed_candle.timestamp.isoformat()},
                            )
                        if completed_candle.timeframe in (
                            Timeframe.M5,
                            Timeframe.M15,
                            Timeframe.H1,
                        ):
                            self._log(
                                "[DIAGNOSTIC] "
                                f"NO CONFLUENCE on "
                                f"TIMEFRAME="
                                f"{completed_candle.timeframe.value}"
                            )

                    if (
                        signal
                        and self.pending_signal is None
                        and self.open_contract_id is None
                    ):

                        ob = signal["order_block"]
                        confluence_ob = signal.get("m5_order_block") or ob

                        try:
                            confluence_row = record_confluence(
                                symbol=self.symbol,
                                timeframe=confluence_ob.timeframe.value,
                                direction=signal["direction"].value.upper(),
                                score=signal.get("score", ""),
                                h1_bias=("BULLISH" if signal["direction"] == BreakDirection.BULLISH else "BEARISH"),
                                liquidity_sweep="YES",
                                m15_structure="YES",
                                m5_fvg="YES" if signal.get("fvg") else "NO",
                                m5_order_block="YES",
                                ob_lower=confluence_ob.lower,
                                ob_upper=confluence_ob.upper,
                                setup_id=str(signal.get("setup_id", "")),
                                quality_score=signal.get("quality", {}).get("score", ""),
                                quality_category=signal.get("quality", {}).get("category", ""),
                                quality_components=json.dumps(signal.get("quality", {}).get("components", {}), separators=(",", ":")),
                                displacement_assessment=json.dumps(signal.get("quality", {}).get("displacement", {}), separators=(",", ":")),
                                premium_discount_state=signal.get("quality", {}).get("premium_discount", {}).get("state", "NOT_AVAILABLE"),
                                tp_source=signal.get("quality", {}).get("tp_source", "NOT_AVAILABLE"),
                                structural_target=signal.get("quality", {}).get("structural_target", ""),
                                potential_rr=signal.get("quality", {}).get("potential_rr", ""),
                                setup_invalidation_reason="",
                                status="FOUND",
                            )
                        except Exception as exc:
                            emit_event("PERSISTENCE_FAILED", severity="ERROR", component="confluence_history",
                                       symbol=self.symbol, timeframe=ob.timeframe.value,
                                       signal_id=str(signal.get("setup_id", "")) or None,
                                       message="Confluence history write failed.", reason=type(exc).__name__,
                                       details={"operation": "record_confluence"})
                            raise

                        self.pending_signal = {
                            "direction": signal["direction"],
                            "order_block": signal["order_block"],
                            "score": signal.get("score", ""),
                            "ticks_waited": 0,
                            "confluence_row": confluence_row,
                            "setup_id": signal.get("setup_id"),
                            "timeframe": ob.timeframe.value,
                            "signal_time": datetime.now(timezone.utc).isoformat(),
                            "htf_poi": signal.get("htf_poi"),
                            "ltf_confirmation": signal.get("ltf_confirmation"),
                            "quality": signal.get("quality", {}),
                        }

                        emit_event(
                            "SIGNAL_GENERATED", component="trader", symbol=self.symbol,
                            timeframe=ob.timeframe.value, signal_id=str(signal.get("setup_id", "")) or None,
                            message="Signal generated and stored as the current pending setup.",
                            details={"score": signal.get("score"), "ob_lower": ob.lower, "ob_upper": ob.upper,
                                     "quality": signal.get("quality", {})},
                        )
                        emit_event(
                            "SIGNAL_ACCEPTED", component="trader", symbol=self.symbol,
                            timeframe=ob.timeframe.value, signal_id=str(signal.get("setup_id", "")) or None,
                            message="Signal entered the existing pending-entry lifecycle.",
                        )
                        self._publish_runtime_status(force=True)

                        self._log(
                            f"SIGNAL "
                            f"{signal['direction'].value.upper()} "
                            f"zone=[{ob.lower}, {ob.upper}] "
                            f"waiting for price to enter zone..."
                        )
                        q = signal.get("quality", {})
                        self._log(f"QUALITY category={q.get('category', 'NOT_AVAILABLE')} score={q.get('score', 'NOT_AVAILABLE')} "
                                  f"components={json.dumps(q.get('components', {}), sort_keys=True)} "
                                  f"tp_source={q.get('tp_source')} potential_rr={q.get('potential_rr')} "
                                  f"displacement={q.get('displacement', {}).get('category')} "
                                  f"premium_discount={q.get('premium_discount', {}).get('state')}")

                    elif signal:
                        reasons = []
                        if self.pending_signal is not None:
                            reasons.append("PENDING_SIGNAL_ALREADY_EXISTS")
                        if self.open_contract_id is not None:
                            reasons.append("OPEN_CONTRACT_ALREADY_EXISTS")
                        emit_event(
                            "SIGNAL_NOT_GENERATED", severity="WARN", component="trader",
                            symbol=self.symbol, timeframe=completed_candle.timeframe.value,
                            signal_id=str(signal.get("setup_id", "")) or None,
                            message="Confluence was found but existing eligibility checks did not create a new pending signal.",
                            reason=",".join(reasons) or "EXISTING_SIGNAL_ELIGIBILITY_CHECK",
                            details={"quality": signal.get("quality", {})},
                        )

            # Watch the live tick for entry.
            if self.pending_signal is not None:
                await self._check_pending_entry(tick)

    @staticmethod
    def _effective_spread(tick) -> float | None:
        try:
            bid = float(tick.bid)
            ask = float(tick.ask)
        except (AttributeError, TypeError, ValueError):
            return None
        if not math.isfinite(bid) or not math.isfinite(ask) or ask < bid:
            return None
        return ask - bid

    @staticmethod
    def _price_increment(tick) -> float:
        pip_size = getattr(tick, "pip_size", None)
        if isinstance(pip_size, int) and pip_size > 0:
            return 10 ** (-pip_size)
        # pip_size is optional in Deriv tick responses. Infer the displayed
        # quote increment from the returned decimal precision when possible.
        exponents = [
            Decimal(str(value)).as_tuple().exponent
            for value in (getattr(tick, "bid", None), getattr(tick, "ask", None), tick.quote)
            if value is not None
        ]
        precision = max(0, -min(exponents)) if exponents else 0
        return 10 ** (-precision)

    def _entry_spread_allowed(self, tick, pending, history_row=None) -> bool:
        spread = self._effective_spread(tick)
        if spread is not None and spread <= self.max_entry_spread:
            return True
        reason = "SPREAD_UNAVAILABLE" if spread is None else "MAX_ENTRY_SPREAD_EXCEEDED"
        if history_row is not None:
            update_trade(history_row, status="REJECTED", error=reason)
        emit_event(
            "SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
            timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
            trade_id=str(history_row) if history_row is not None else None,
            message="New entry rejected by the bid/ask execution-quality guard.",
            reason=reason,
            details={"effective_spread": spread, "max_entry_spread": self.max_entry_spread},
        )
        self._log(f"ENTRY REJECTED: effective_spread={spread} max_entry_spread={self.max_entry_spread}")
        self.pending_signal = None
        self._publish_runtime_status(force=True)
        return False

    def _structural_trailing_candidate(self, state: ProtectionState) -> float | None:
        if not state.trailing_enabled:
            return None
        detector = self.market_service.bos_detectors[Timeframe.M1]
        structure = self.market_service.structures[Timeframe.M1]
        directional_breaks = [
            item for item in detector.breaks
            if item.direction == state.direction and event_time(item.candle) > state.opened_at
        ]
        if not directional_breaks:
            return None
        latest_break_time = event_time(directional_breaks[-1].candle)
        if state.direction == BreakDirection.BULLISH:
            points = [
                point for point in structure.swing_lows
                if event_time(point.candle) > latest_break_time
            ]
            level = max((point.candle.low for point in points), default=None)
            return level - state.initial_spread if level is not None else None
        points = [
            point for point in structure.swing_highs
            if event_time(point.candle) > latest_break_time
        ]
        level = min((point.candle.high for point in points), default=None)
        return level + state.initial_spread if level is not None else None

    async def _manage_open_positions(self, tick) -> None:
        """Tick-driven local BE/trailing protection; existing broker limits stay active."""
        for contract_id, state in list(self.open_trade_protection.items()):
            structural_stop = self._structural_trailing_candidate(state)
            action = update_protection(state, tick.quote, structural_stop)
            if action == "breakeven_armed":
                emit_event(
                    "BREAKEVEN_PROTECTION_ARMED", component="trader", symbol=self.symbol,
                    contract_id=contract_id,
                    message="Local protective close level armed after the configured R threshold.",
                    details={"entry_price": state.entry_price,
                             "protective_stop_price": state.protective_stop_price,
                             "trigger_r": state.be_trigger_r},
                )
            elif action == "trailing_stop_advanced":
                emit_event(
                    "STRUCTURAL_TRAIL_ADVANCED", component="trader", symbol=self.symbol,
                    contract_id=contract_id,
                    message="Local protective close level advanced behind confirmed M1 structure.",
                    details={"protective_stop_price": state.protective_stop_price},
                )
            elif action == "close":
                try:
                    await self.trading_client.close_contract(int(contract_id))
                    emit_event(
                        "PROTECTIVE_CLOSE_SUBMITTED", component="trader", symbol=self.symbol,
                        contract_id=contract_id,
                        message="Protective price level crossed; contract close requested from Deriv.",
                        details={"quote": tick.quote, "protective_stop_price": state.protective_stop_price},
                    )
                except Exception as exc:
                    # Do not blindly retry an ambiguous sell request; contract
                    # subscription reconciliation remains the source of truth.
                    emit_event(
                        "PROTECTIVE_CLOSE_FAILED", severity="ERROR", component="deriv",
                        symbol=self.symbol, contract_id=contract_id,
                        message="Protective close request failed; the original broker monetary stop remains active.",
                        reason=type(exc).__name__,
                    )

    async def _check_pending_entry(self, tick):
        pending = self.pending_signal
        ob = pending["order_block"]

        pending["ticks_waited"] += 1

        if pending["ticks_waited"] > self.max_pending_ticks:
            self._log("PENDING SIGNAL EXPIRED (price never reached the zone)")
            emit_event(
                "SIGNAL_EXPIRED", severity="WARN", component="trader", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                message="Pending signal expired because price did not enter its zone.",
                reason="PENDING_TICK_LIMIT_REACHED",
                details={"ticks_waited": pending["ticks_waited"]},
            )
            self.pending_signal = None
            self._publish_runtime_status(force=True)
            return

        price = tick.quote
        if not (ob.lower <= price <= ob.upper):
            return

        entry_price = price
        bullish = pending["direction"] == BreakDirection.BULLISH
        stop_loss_price = ob.lower if bullish else ob.upper
        risk_distance = abs(entry_price - stop_loss_price)
        price_increment = self._price_increment(tick)
        target_candidates = rank_structural_targets(
            entry_price, pending["direction"], tick.timestamp,
            self.market_service.structures, self.market_service.liquidity_detectors,
            self.market_service.fvg_detectors, price_increment, stop_price=stop_loss_price,
        )
        eligible_targets = [item for item in target_candidates
                            if item.get("potential_rr", 0) >= self.min_entry_rr]
        target_info = eligible_targets[0] if eligible_targets else (target_candidates[0] if target_candidates else None)
        selected_target_fails_minimum = bool(target_candidates and not eligible_targets)
        take_profit_price = target_info["level"] if target_info else None
        if take_profit_price is None:
            # Preserve the existing 2R target when no confirmed opposing
            # structural liquidity is available in the closed-candle state.
            take_profit_price = entry_price + 2 * risk_distance if bullish else entry_price - 2 * risk_distance
            target_info = {"level": take_profit_price, "source": "2R_fallback", "potential_rr": 2.0,
                           "quality_score": 5, "kind": "fallback"}
        reward_distance = abs(take_profit_price - entry_price)
        reward_multiple = reward_distance / risk_distance if risk_distance > 0 else 2.0
        if reward_multiple < self.min_entry_rr or selected_target_fails_minimum:
            reason = "MINIMUM_ENTRY_RR_NOT_MET"
            self._log(f"ENTRY REJECTED: potential_rr={reward_multiple:.3f} minimum={self.min_entry_rr:.3f} "
                      f"tp_source={target_info.get('source')}")
            emit_event("SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
                       timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                       message="Selected structural target does not meet the configured minimum entry R:R.",
                       reason=reason, details={"potential_rr": reward_multiple, "minimum_rr": self.min_entry_rr,
                                               "tp_source": target_info.get("source"), "target": take_profit_price})
            if pending.get("confluence_row") is not None:
                update_confluence(pending["confluence_row"], status="REJECTED", rejection_reason=reason,
                                  potential_rr=reward_multiple, tp_source=target_info.get("source"),
                                  structural_target=(target_info.get("level") if target_info.get("kind") != "fallback" else ""),
                                  quality_score=pending.get("quality", {}).get("score", ""),
                                  quality_category=pending.get("quality", {}).get("category", ""))
            self.pending_signal = None
            self._publish_runtime_status(force=True)
            return

        quality = pending.get("quality", {})
        quality["potential_rr"] = reward_multiple
        quality["tp_source"] = target_info.get("source", "NOT_AVAILABLE")
        quality["structural_target"] = take_profit_price if target_info.get("kind") != "fallback" else "NOT_AVAILABLE"
        quality["target_assessment"] = target_info
        quality.setdefault("components", {})["potential_rr"] = min(10, max(0, int(reward_multiple * 3)))
        quality.setdefault("components", {})["target_quality"] = min(7, max(0, int(target_info.get("quality_score", 0) / 7)))
        quality["score"] = min(100, max(0, sum(quality["components"].values())))
        quality["category"] = "A+" if quality["score"] >= 90 else "A" if quality["score"] >= 80 else "B" if quality["score"] >= 65 else "C"
        if pending.get("confluence_row") is not None:
            update_confluence(
                pending["confluence_row"], potential_rr=reward_multiple,
                tp_source=quality["tp_source"], structural_target=quality["structural_target"],
                quality_score=quality["score"], quality_category=quality["category"],
                quality_components=json.dumps(quality["components"], separators=(",", ":")),
                displacement_assessment=json.dumps(quality.get("displacement", {}), separators=(",", ":")),
                premium_discount_state=quality.get("premium_discount", {}).get("state", "NOT_AVAILABLE"),
            )

        def size_for(multiplier: float):
            return compute_stake(
                entry_price=entry_price,
                stop_loss_price=stop_loss_price,
                multiplier=multiplier,
                risk_amount=self.risk_amount,
                reward_multiple=reward_multiple,
                min_stake=self.min_stake,
                max_stake=self.max_stake,
            )

        sized = size_for(self.multiplier)

        if sized is None:
            self._log(
                f"SKIPPING TRADE: stop distance cannot support "
                f"${self.risk_amount:.2f} risk within stake limits "
                f"(${self.min_stake:.2f}-${self.max_stake:.2f})"
            )
            emit_event(
                "SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                message="Pending signal could not be sized within configured stake limits.",
                reason="STOP_DISTANCE_CANNOT_SUPPORT_RISK_WITHIN_STAKE_LIMITS",
                details={"risk_amount": self.risk_amount, "min_stake": self.min_stake, "max_stake": self.max_stake},
            )
            self.pending_signal = None
            self._publish_runtime_status(force=True)
            return

        if not self._entry_spread_allowed(tick, pending):
            return
        entry_spread = self._effective_spread(tick)

        direction_str = "up" if bullish else "down"

        reward_distance = abs(take_profit_price - entry_price)
        technical_rr = (
            reward_distance / risk_distance
            if risk_distance > 0
            else NOT_AVAILABLE
        )

        try:
            history_row = record_trade(
            symbol=self.symbol,
            direction=direction_str.upper(),
            score=pending.get("score", ""),
            entry=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            stop_distance_pct=f"{sized.stop_distance_pct:.8f}",
            stake=sized.stake,
            multiplier=sized.multiplier,
            risk_amount=sized.stop_loss_amount,
            reward_amount=sized.take_profit_amount,
            entry_price=entry_price,
            technical_sl_price=stop_loss_price,
            technical_tp_price=take_profit_price,
            technical_stop_distance=risk_distance,
            technical_reward_distance=reward_distance,
            technical_rr=technical_rr,
            quality_score=quality.get("score", ""),
            quality_category=quality.get("category", ""),
            quality_components=json.dumps(quality.get("components", {}), separators=(",", ":")),
            displacement_assessment=json.dumps(quality.get("displacement", {}), separators=(",", ":")),
            premium_discount_state=quality.get("premium_discount", {}).get("state", "NOT_AVAILABLE"),
            tp_source=quality.get("tp_source", "NOT_AVAILABLE"),
            structural_target=quality.get("structural_target", "NOT_AVAILABLE"),
            potential_rr=reward_multiple,
            setup_invalidation_reason="",
            deriv_stop_loss_amount=sized.stop_loss_amount,
            deriv_take_profit_amount=sized.take_profit_amount,
            broker_stop_loss_value=NOT_AVAILABLE,
            broker_take_profit_value=NOT_AVAILABLE,
            contract_created_at=NOT_AVAILABLE,
            contract_updated_at=NOT_AVAILABLE,
            final_pl=NOT_AVAILABLE,
                status="ORDER_SUBMITTED",
            )
        except Exception as exc:
            emit_event(
                "PERSISTENCE_FAILED", severity="ERROR", component="trade_history", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                message="Trade journal write failed before broker submission.", reason=type(exc).__name__,
                details={"operation": "record_trade"},
            )
            raise

        self._log(
            f"ENTERING {direction_str.upper()} at {entry_price:.4f} "
            f"stake=${sized.stake:.2f} multiplier={sized.multiplier:g} "
            f"deriv_stop_loss_amount=${sized.stop_loss_amount:.2f} "
            f"deriv_take_profit_amount=${sized.take_profit_amount:.2f}"
        )

        emit_event(
            "ORDER_SUBMITTED", component="trader", symbol=self.symbol,
            timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
            trade_id=str(history_row), message="Order request sent to Deriv.",
            details={"direction": direction_str.upper(), "stake": sized.stake,
                     "multiplier": sized.multiplier, "effective_spread": entry_spread},
        )

        try:
            if not self._entry_spread_allowed(tick, pending, history_row):
                return
            response = await self.trading_client.buy_multiplier(
                symbol=self.symbol,
                direction=direction_str,
                stake=sized.stake,
                multiplier=sized.multiplier,
                stop_loss=sized.stop_loss_amount,
                take_profit=sized.take_profit_amount,
                reference_quote=entry_price,
                max_quote_deviation=self.max_entry_spread,
            )

        except ExecutionPriceMovedError as exc:
            update_trade(history_row, status="REJECTED", error=str(exc))
            emit_event(
                "SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                trade_id=str(history_row), message="Proposal price moved outside the configured execution tolerance.",
                reason="PROPOSAL_SPOT_MOVED",
            )
            self.pending_signal = None
            self._publish_runtime_status(force=True)
            return

        except MultiplierOutOfRangeError as exc:
            emit_event(
                "BROKER_REJECTED", severity="WARN", component="deriv", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                trade_id=str(history_row), message="Deriv rejected the requested multiplier; existing fallback is being evaluated.",
                reason="MULTIPLIER_OUT_OF_RANGE",
                details={"requested_multiplier": self.multiplier, "allowed_multipliers": exc.allowed},
            )
            # Deriv explicitly tells us which multipliers this symbol accepts.
            # Pick the largest accepted value not greater than our preferred
            # multiplier, then recalculate the stake so the $5 risk target is
            # preserved. This rejection creates no contract, so retrying is safe.
            candidates = [m for m in exc.allowed if m <= self.multiplier]
            fallback_multiplier = max(candidates) if candidates else min(exc.allowed)
            fallback_sized = size_for(fallback_multiplier)

            if fallback_sized is None:
                update_trade(
                    history_row,
                    status="REJECTED",
                    error=f"Multiplier {self.multiplier:g} invalid; allowed={exc.allowed}; "
                    "fallback could not achieve requested risk within stake limits",
                )
                self._log(
                    f"ORDER SKIPPED: {self.symbol} accepts {exc.allowed}; "
                    f"fallback multiplier {fallback_multiplier:g} cannot achieve "
                    f"${self.risk_amount:.2f} risk within stake limits"
                )
                emit_event(
                    "SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
                    timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                    message="Deriv rejected the requested multiplier and the existing fallback could not satisfy stake limits.",
                    reason="FALLBACK_MULTIPLIER_OUTSIDE_STAKE_LIMITS",
                    details={"requested_multiplier": self.multiplier, "allowed_multipliers": exc.allowed},
                )
                self.pending_signal = None
                self._publish_runtime_status(force=True)
                return

            self._log(
                f"Multiplier {self.multiplier:g} rejected for {self.symbol}; "
                f"retrying with {fallback_multiplier:g}"
            )

            emit_event(
                "ORDER_SUBMITTED", component="trader", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                trade_id=str(history_row), message="Fallback order request sent to Deriv.",
                details={"attempt": 2, "direction": direction_str.upper(),
                         "stake": fallback_sized.stake, "multiplier": fallback_multiplier},
            )

            sized = fallback_sized
            try:
                if not self._entry_spread_allowed(tick, pending, history_row):
                    return
                response = await self.trading_client.buy_multiplier(
                    symbol=self.symbol,
                    direction=direction_str,
                    stake=sized.stake,
                    multiplier=sized.multiplier,
                    stop_loss=sized.stop_loss_amount,
                    take_profit=sized.take_profit_amount,
                    reference_quote=entry_price,
                    max_quote_deviation=self.max_entry_spread,
                )
            except Exception as retry_exc:
                update_trade(
                    history_row,
                    status="REJECTED",
                    error=str(retry_exc),
                )
                if isinstance(retry_exc, ExecutionPriceMovedError):
                    emit_event(
                        "SIGNAL_REJECTED", severity="WARN", component="trader", symbol=self.symbol,
                        timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                        trade_id=str(history_row), message="Fallback proposal price moved outside the configured execution tolerance.",
                        reason="PROPOSAL_SPOT_MOVED",
                    )
                else:
                    self._log(f"DEMO ORDER FAILED: {retry_exc}")
                    self.last_error = f"{type(retry_exc).__name__}: {retry_exc}"
                    emit_event(
                        "ORDER_FAILED", severity="ERROR", component="deriv", symbol=self.symbol,
                        timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                        trade_id=str(history_row), message="Fallback order request failed before a contract response was returned.",
                        reason=str(retry_exc),
                    )
                self.pending_signal = None
                self._publish_runtime_status(force=True)
                return

        except Exception as exc:
            update_trade(
                history_row,
                status="REJECTED",
                error=str(exc),
            )
            self._log(f"DEMO ORDER FAILED: {exc}")
            self.last_error = f"{type(exc).__name__}: {exc}"
            emit_event(
                "ORDER_FAILED", severity="ERROR", component="deriv", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                trade_id=str(history_row), message="Order flow failed before a contract response was returned.", reason=str(exc),
            )
            self.pending_signal = None
            self._publish_runtime_status(force=True)
            return

        buy_info = response.get("buy", {})
        contract_id = buy_info.get("contract_id")
        if contract_id:
            contract_id = str(contract_id)
            self.open_contracts[contract_id] = history_row
            self.open_trade_protection[contract_id] = ProtectionState(
                direction=pending["direction"],
                entry_price=entry_price,
                initial_stop_price=stop_loss_price,
                initial_spread=entry_spread or 0.0,
                opened_at=tick.timestamp,
                be_trigger_r=self.breakeven_trigger_r,
                breakeven_enabled=self.breakeven_enabled,
                trailing_enabled=self.structural_trailing_enabled,
            )
        transaction_id = buy_info.get("transaction_id")
        contract_update = response.get("contract_update", {})
        stop_loss_update = (
            contract_update.get("stop_loss", {})
            if isinstance(contract_update, dict)
            else {}
        )
        take_profit_update = (
            contract_update.get("take_profit", {})
            if isinstance(contract_update, dict)
            else {}
        )
        broker_stop_loss_value = broker_value(stop_loss_update)
        broker_take_profit_value = broker_value(take_profit_update)
        created_at = contract_created_at(buy_info)
        updated_at = contract_updated_at(contract_update)

        update_trade(
            history_row,
            status="OPEN",
            stake=sized.stake,
            multiplier=sized.multiplier,
            risk_amount=sized.stop_loss_amount,
            reward_amount=sized.take_profit_amount,
            entry_price=entry_price,
            deriv_stop_loss_amount=sized.stop_loss_amount,
            deriv_take_profit_amount=sized.take_profit_amount,
            broker_stop_loss_value=broker_stop_loss_value,
            broker_take_profit_value=broker_take_profit_value,
            contract_created_at=created_at,
            contract_updated_at=updated_at,
            final_pl=NOT_AVAILABLE,
            contract_id=contract_id,
            transaction_id=transaction_id,
        )

        self.pending_signal = None
        self.last_error = None
        self._publish_runtime_status(force=True)

        if contract_id:
            emit_event(
                "CONTRACT_OPENED", component="trader", symbol=self.symbol,
                timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
                trade_id=str(history_row), contract_id=contract_id,
                message="Deriv returned a contract ID for the opened contract.",
                details={"transaction_id": transaction_id, "entry_price": entry_price},
            )
        else:
            emit_event(
                "BROKER_RESPONSE_MISSING_CONTRACT_ID", severity="ERROR", component="deriv",
                symbol=self.symbol, timeframe=pending.get("timeframe"),
                signal_id=str(pending.get("setup_id", "")) or None, trade_id=str(history_row),
                message="Order response did not include a contract ID.",
            )
        emit_event(
            "SLTP_APPLIED", component="deriv", symbol=self.symbol,
            timeframe=pending.get("timeframe"), signal_id=str(pending.get("setup_id", "")) or None,
            trade_id=str(history_row), contract_id=contract_id,
            message="Deriv returned successfully from the monetary stop/target update request.",
            details={"broker_stop_loss_value": broker_stop_loss_value,
                     "broker_take_profit_value": broker_take_profit_value,
                     "deriv_stop_loss_amount": sized.stop_loss_amount,
                     "deriv_take_profit_amount": sized.take_profit_amount},
        )

        self._log(
            f"TRADE OPENED | contract={contract_id} | "
            f"multiplier={sized.multiplier:g} | "
            f"deriv_stop_loss_amount=${sized.stop_loss_amount:.2f} | "
            f"deriv_take_profit_amount=${sized.take_profit_amount:.2f}"
        )
        self._log(
            "TRADE RISK "
            f"contract_id={contract_id or NOT_AVAILABLE} "
            f"transaction_id={transaction_id or NOT_AVAILABLE} "
            f"symbol={self.symbol} direction={direction_str.upper()} "
            f"entry_price={entry_price} "
            f"technical_sl_price={stop_loss_price} "
            f"technical_tp_price={take_profit_price} "
            f"technical_stop_distance={risk_distance} "
            f"technical_reward_distance={reward_distance} "
            f"technical_rr={technical_rr} "
            f"risk_amount={sized.stop_loss_amount} "
            f"reward_amount={sized.take_profit_amount} "
            f"stake={sized.stake} multiplier={sized.multiplier} "
            f"deriv_stop_loss_amount={sized.stop_loss_amount} "
            f"deriv_take_profit_amount={sized.take_profit_amount} "
            f"quality_score={quality.get('score', NOT_AVAILABLE)} "
            f"quality_category={quality.get('category', NOT_AVAILABLE)} "
            f"quality_components={json.dumps(quality.get('components', {}), sort_keys=True)} "
            f"tp_source={quality.get('tp_source', NOT_AVAILABLE)} "
            f"structural_target={quality.get('structural_target', NOT_AVAILABLE)} "
            f"potential_rr={reward_multiple} "
            f"displacement={quality.get('displacement', {}).get('category', NOT_AVAILABLE)} "
            f"premium_discount={quality.get('premium_discount', {}).get('state', NOT_AVAILABLE)} "
            f"broker_stop_loss_value={broker_stop_loss_value} "
            f"broker_take_profit_value={broker_take_profit_value} "
            f"contract_created_at={created_at} "
            f"contract_updated_at={updated_at} final_pl={NOT_AVAILABLE}"
        )

        if contract_id:
            asyncio.create_task(
                self._watch_open_contract(
                    contract_id,
                    history_row,
                )
            )

    async def _watch_open_contract(self, contract_id, history_row):

        self._log(
            f"Watching DEMO contract "
            f"{contract_id}..."
        )

        async for update in self.trading_client.subscribe_contract(
            contract_id
        ):

            if update.get("is_sold"):

                profit = update.get("profit")
                status = "WIN" if float(profit or 0) > 0 else "LOSS" if float(profit or 0) < 0 else "BREAKEVEN"
                final_pl = profit if profit is not None else NOT_AVAILABLE
                sell_time = update.get("sell_time")
                try:
                    exit_time = datetime.fromtimestamp(
                        float(sell_time),
                        timezone.utc,
                    ).isoformat() if sell_time is not None else ""
                except (TypeError, ValueError, OSError, OverflowError):
                    exit_time = ""
                update_trade(
                    history_row,
                    status=status,
                    exit_time=exit_time,
                    profit=profit,
                    final_pl=final_pl,
                )
                self._log(
                    f"CONTRACT CLOSED contract_id={contract_id} "
                    f"status={status} final_pl={final_pl}"
                )
                emit_event(
                    "CONTRACT_CLOSED", component="trader", symbol=self.symbol,
                    trade_id=str(history_row), contract_id=contract_id,
                    message="Deriv reported the contract closed.",
                    details={"status": status, "final_pl": final_pl},
                )

                self.open_contracts.pop(str(contract_id), None)
                self.open_trade_protection.pop(str(contract_id), None)
                self._publish_runtime_status(force=True)

                self._log(
                    "Watch Tower is ready for the next setup on this symbol."
                )


class WatchTower:
    """
    Runs a LiveTrader for every symbol in `symbols` concurrently, all
    sharing one authenticated DEMO connection.

    Each symbol is fully independent: its own candles, its own
    confluence signals, its own pending/open trade. JD75 being in a
    trade has no effect on whether R_75 or JD10 can open one too.
    """

    def __init__(
        self,
        symbols: list[str],
        app_id: str,
        api_token: str,
        demo_account_id: str,
        multiplier: float = 100,
        risk_amount: float = 5.00,
        min_stake: float = 1.0,
        max_stake: float = 100.0,
        max_pending_ticks: int = 3000,
        max_entry_spread: float = 1.0,
        breakeven_enabled: bool = True,
        breakeven_trigger_r: float = 1.5,
        structural_trailing_enabled: bool = False,
        min_entry_rr: float = 0.0,
        restart_delay_seconds: float = 10.0,
    ):
        if not symbols:
            raise ValueError("WatchTower needs at least one symbol.")

        self.symbols = list(symbols)
        self.demo_account_id = demo_account_id
        self.restart_delay_seconds = restart_delay_seconds

        self.trading_client = DerivTradingClient(
            app_id,
            api_token,
        )

        self._trader_kwargs = dict(
            multiplier=multiplier,
            risk_amount=risk_amount,
            min_stake=min_stake,
            max_stake=max_stake,
            max_pending_ticks=max_pending_ticks,
            max_entry_spread=max_entry_spread,
            breakeven_enabled=breakeven_enabled,
            breakeven_trigger_r=breakeven_trigger_r,
            structural_trailing_enabled=structural_trailing_enabled,
            min_entry_rr=min_entry_rr,
        )

    async def start(self):
        accounts = self.trading_client.list_accounts()

        selected_account = self.demo_account_id
        account_label = f"…{selected_account[-3:]}" if len(selected_account) > 3 else "REDACTED"
        instance_id = os.environ.get("INSTANCE_ID", "unconfigured")
        print(
            f"[INSTANCE={instance_id}] Loaded {len(accounts)} account(s); "
            f"using configured DEMO account {account_label}"
        )

        await self.trading_client.connect(
            self.demo_account_id
        )

        balance = await self.trading_client.get_balance()
        balance_data = balance.get("balance", balance)
        if isinstance(balance_data, dict):
            balance_display = {
                key: balance_data[key]
                for key in ("balance", "currency")
                if key in balance_data
            }
        else:
            balance_display = balance_data
        self.trading_client.last_balance = balance_display

        emit_event(
            "DERIV_BALANCE_READ", component="deriv",
            message="Initial authenticated account balance read succeeded.",
            details={"balance": balance_display.get("balance") if isinstance(balance_display, dict) else balance_display,
                     "currency": balance_display.get("currency") if isinstance(balance_display, dict) else None,
                     "account_id": account_label, "account_type": "demo"},
        )

        print(
            f"[INSTANCE={instance_id}] Connected. DEMO balance snapshot: "
            f"{balance_display}"
        )

        print(
            f"[INSTANCE={instance_id}] *** DEMO LIVE EXECUTION ENABLED ***"
        )

        print(
            f"[INSTANCE={instance_id}] Watching {len(self.symbols)} symbol(s) independently: "
            f"{', '.join(self.symbols)}"
        )

        print(
            f"[INSTANCE={instance_id}] Each symbol can hold its own open trade at the same "
            "time as the others."
        )

        await asyncio.gather(
            *(
                self._run_symbol_forever(symbol)
                for symbol in self.symbols
            )
        )

    async def _run_symbol_forever(self, symbol: str):
        """
        Keeps a symbol under watch even if its own watcher hits an
        unexpected error. Other symbols are unaffected, since each
        LiveTrader only shares the trading connection, not state.

        Deriv connection issues are already handled inside
        MarketDataService/DerivClient (tick stream) and
        DerivTradingClient (trading requests) -- this loop only
        catches things beyond that, e.g. a bug, and keeps the symbol
        from silently going dark.
        """

        while True:

            trader = LiveTrader(
                symbol=symbol,
                trading_client=self.trading_client,
                **self._trader_kwargs,
            )

            try:
                await trader.start()

                # start()/_run() only return if the tick stream
                # generator itself ends, which DerivClient doesn't
                # currently do on purpose -- treat it as done.
                return

            except asyncio.CancelledError:
                raise

            except Exception as exc:

                emit_event(
                    "SYMBOL_WATCHER_CRASHED", severity="ERROR", component="trader",
                    symbol=symbol, message="Symbol watcher exited unexpectedly and will restart.",
                    reason=type(exc).__name__, details={"error": str(exc)},
                )

                print(
                    f"[INSTANCE={os.environ.get('INSTANCE_ID', 'unconfigured')}][{symbol}] !!! WATCHER CRASHED: {exc} !!!"
                )

                print(
                    f"[INSTANCE={os.environ.get('INSTANCE_ID', 'unconfigured')}][{symbol}] Restarting this symbol's watcher "
                    f"in {self.restart_delay_seconds:.0f}s "
                    "(other symbols are unaffected)..."
                )

                await asyncio.sleep(
                    self.restart_delay_seconds
                )


if __name__ == "__main__":
    import config
    from instance_config import acquire_trader_lock

    _TRADER_INSTANCE_LOCK = acquire_trader_lock()

    tower = WatchTower(
        symbols=config.SYMBOLS,
        app_id=config.APP_ID,
        api_token=config.API_TOKEN,
        demo_account_id=config.DEMO_ACCOUNT_ID,
        multiplier=100,
        risk_amount=5.00,
        min_stake=1.0,
        max_stake=100.0,
        max_pending_ticks=3000,
        max_entry_spread=config.MAX_ENTRY_SPREAD,
        breakeven_enabled=config.BREAKEVEN_ENABLED,
        breakeven_trigger_r=config.BREAKEVEN_TRIGGER_R,
        structural_trailing_enabled=config.STRUCTURAL_TRAILING_ENABLED,
        min_entry_rr=config.MIN_ENTRY_RR,
    )

    asyncio.run(
        tower.start()
    )
