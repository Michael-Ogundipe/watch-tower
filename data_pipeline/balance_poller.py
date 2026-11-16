"""Record the broker's own account figures, on a timer.

Runs as its own process beside the trader. It reads and never writes to
Deriv: balance, the open portfolio, and each open contract's current value.
It places no orders and touches no trading state, so it cannot affect the
strategy.

    balance  what Deriv reports for the account
    equity   that balance plus the unrealised profit of open contracts
    currency as reported

Each reading is stored in balance_snapshots, which becomes the history
behind the dashboard's account section and, later, a balance chart.

    python balance_poller.py

WATCHTOWER_BALANCE_INTERVAL sets the seconds between readings (default 300).
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import config
import db_sink
from deriv_trading_client import DerivTradingClient
from ops_observability import emit_event, emit_runtime_status

INTERVAL = int(os.environ.get("WATCHTOWER_BALANCE_INTERVAL", "300"))
# A failed reading should be retried sooner than the normal cadence, but not
# so fast that a broker outage turns into a hammering loop.
RETRY_SECONDS = 60
CONTRACT_TIMEOUT = 20
PROCESS_STARTED_AT = datetime.now(timezone.utc)
LAST_SUCCESSFUL_POLL: str | None = None
LAST_POLL_ERROR: str | None = None


def _publish_runtime(*, running=True, last_error=None, balance=None, currency=None, open_count=None):
    now = datetime.now(timezone.utc)
    emit_runtime_status("balance", {
        "running": running,
        "process_started_at": PROCESS_STARTED_AT.isoformat(),
        "process_uptime_seconds": max(0, int((now - PROCESS_STARTED_AT).total_seconds())),
        "last_successful_poll": LAST_SUCCESSFUL_POLL,
        "last_error": last_error,
        "balance": balance,
        "currency": currency,
        "open_contract_count": open_count,
    })


def log(message: str) -> None:
    print(
        f"[INSTANCE={config.INSTANCE_ID}][balance] "
        f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {message}",
        flush=True,
    )


async def _open_positions(client: DerivTradingClient) -> tuple[int, float | None]:
    """How many contracts are open, and their combined unrealised profit.

    The portfolio call lists contracts but does not value them, so each one
    is asked for its current figure. If any contract cannot be valued the
    total is returned as None rather than a partial sum, because a partial
    equity would be misleading.
    """
    portfolio = await client.get_portfolio()
    contracts = (portfolio.get("portfolio") or {}).get("contracts") or []
    if not contracts:
        return 0, 0.0

    total = 0.0
    for contract in contracts:
        contract_id = contract.get("contract_id")
        if contract_id is None:
            return len(contracts), None
        try:
            reply = await asyncio.wait_for(
                client._send(
                    {"proposal_open_contract": 1, "contract_id": int(contract_id)},
                    expected_msg_type="proposal_open_contract",
                ),
                timeout=CONTRACT_TIMEOUT,
            )
        except Exception as exc:
            log(f"could not value contract {contract_id}: {type(exc).__name__}")
            emit_event("OPEN_CONTRACT_VALUATION_FAILED", severity="WARN", component="balance",
                       contract_id=str(contract_id), message="Balance worker could not value an open contract.",
                       reason=type(exc).__name__)
            return len(contracts), None

        profit = (reply.get("proposal_open_contract") or {}).get("profit")
        if profit is None:
            return len(contracts), None
        try:
            total += float(profit)
        except (TypeError, ValueError):
            return len(contracts), None

    return len(contracts), round(total, 6)


async def take_snapshot(client: DerivTradingClient) -> None:
    """One reading, stored as the broker reported it."""
    global LAST_SUCCESSFUL_POLL, LAST_POLL_ERROR
    reply = await client.get_balance()
    payload = reply.get("balance") or {}
    raw_balance = payload.get("balance")
    if raw_balance is None:
        log("balance missing from the reply; nothing recorded")
        return

    balance = float(raw_balance)
    currency = payload.get("currency")
    account_id = payload.get("loginid") or client.account_id

    open_count, open_profit = await _open_positions(client)
    equity = round(balance + open_profit, 6) if open_profit is not None else None

    db_sink.record_balance(
        balance=balance,
        currency=currency,
        equity=equity,
        open_contracts=open_count,
        open_profit=open_profit,
        account_id=account_id,
    )

    LAST_SUCCESSFUL_POLL = datetime.now(timezone.utc).isoformat()
    LAST_POLL_ERROR = None
    _publish_runtime(balance=balance, currency=currency, open_count=open_count)
    emit_event(
        "BALANCE_POLL_SUCCEEDED", component="balance",
        message="Read-only Deriv balance and portfolio poll succeeded.",
        details={"balance": balance, "currency": currency, "open_contract_count": open_count,
                 "account_id": client._masked_account_id(account_id)},
    )

    equity_text = f"{equity:.2f}" if equity is not None else "unavailable"
    log(
        f"balance={balance:.2f} {currency} equity={equity_text} "
        f"open={open_count}"
    )


async def main() -> None:
    global LAST_POLL_ERROR
    log(f"starting; reading every {INTERVAL}s")
    emit_event("BALANCE_WORKER_STARTING", component="balance", message="Balance poller starting.")
    _publish_runtime(last_error=None)
    client = DerivTradingClient(config.APP_ID, config.API_TOKEN)
    try:
        client.list_accounts()
        await client.connect(config.DEMO_ACCOUNT_ID)
    except Exception as exc:
        LAST_POLL_ERROR = f"{type(exc).__name__}: {exc}"
        _publish_runtime(running=False, last_error=LAST_POLL_ERROR)
        emit_event("BALANCE_WORKER_CONNECT_FAILED", severity="ERROR", component="balance",
                   message="Balance worker could not authenticate to Deriv.", reason=type(exc).__name__)
        raise
    log(f"connected to the configured DEMO account, reading only")
    emit_event("BALANCE_WORKER_STARTED", component="balance", message="Balance worker authenticated and polling.")

    while True:
        try:
            await take_snapshot(client)
            await asyncio.sleep(INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f"reading failed ({type(exc).__name__}: {exc}); retrying in {RETRY_SECONDS}s")
            LAST_POLL_ERROR = f"{type(exc).__name__}: {exc}"
            _publish_runtime(last_error=LAST_POLL_ERROR)
            emit_event("BALANCE_POLL_FAILED", severity="ERROR", component="balance",
                       message="Read-only Deriv balance poll failed; retry scheduled.",
                       reason=type(exc).__name__, details={"error": str(exc), "retry_seconds": RETRY_SECONDS})
            await asyncio.sleep(RETRY_SECONDS)
            try:
                # The client reconnects itself, but a hard failure needs a
                # fresh authenticated socket.
                await client.connect(config.DEMO_ACCOUNT_ID)
            except Exception as reconnect_error:
                log(f"reconnect failed ({type(reconnect_error).__name__}); will try again")
                LAST_POLL_ERROR = f"{type(reconnect_error).__name__}: {reconnect_error}"
                _publish_runtime(last_error=LAST_POLL_ERROR)
                emit_event("BALANCE_WORKER_RECONNECT_FAILED", severity="ERROR", component="balance",
                           message="Balance worker Deriv reconnect failed.", reason=type(reconnect_error).__name__)


if __name__ == "__main__":
    asyncio.run(main())
