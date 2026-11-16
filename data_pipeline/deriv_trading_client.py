"""
Trading client for Deriv's current Options API.

Authenticated flow:
    REST accounts -> REST OTP -> authenticated WebSocket
    proposal -> buy -> contract_update -> proposal_open_contract

The client uses one WebSocket reader, request IDs, application-level
keepalive pings, and automatic re-authentication/reconnects.
"""

import asyncio
import json
import math
import os
import re
from datetime import datetime, timezone

import requests
import websockets
from trade_observability import broker_value
from ops_observability import emit_event


REST_BASE_URL = "https://api.derivws.com"

# Deriv recommends application-level pings every 30-60 seconds.
KEEPALIVE_INTERVAL = 30.0
PING_TIMEOUT = 10.0
RECONNECT_TIMEOUT = 15.0
CONTRACT_RECONCILE_INTERVAL = 30.0

# Set True only when detailed Deriv API debugging is needed.
DEBUG_DERIV_API = False


class MultiplierOutOfRangeError(RuntimeError):
    """Raised when Deriv rejects a multiplier and provides allowed values."""

    def __init__(self, symbol: str, requested: float, allowed: list[float]):
        self.symbol = symbol
        self.requested = requested
        self.allowed = allowed
        super().__init__(
            f"Multiplier {requested} is not accepted for {symbol}. "
            f"Allowed: {allowed}"
        )


class ExecutionPriceMovedError(RuntimeError):
    """Proposal spot moved beyond the caller's entry-price tolerance."""


class DerivTradingClient:
    def __init__(
        self,
        app_id: str,
        api_token: str,
    ):
        self.app_id = app_id
        self.api_token = api_token

        self.websocket = None

        # Incremented whenever a new authenticated socket is installed.
        # Contract subscriptions use this to detect a reconnect and request
        # a fresh proposal_open_contract snapshot instead of waiting forever
        # on an update stream attached to the old socket.
        self._connection_generation = 0

        self.account_id: str | None = None
        self.authenticated = False
        self.reconnect_count = 0
        self.last_authenticated_at: str | None = None

        self.accounts_by_id: dict[str, dict] = {}

        # ---------------------------------------------------------
        # Request / response routing
        # ---------------------------------------------------------

        self._next_req_id = 1

        self._pending_requests: dict[
            int,
            asyncio.Future,
        ] = {}

        self._receiver_task: asyncio.Task | None = None

        # ---------------------------------------------------------
        # Keepalive
        # ---------------------------------------------------------

        self._keepalive_task: asyncio.Task | None = None

        # ---------------------------------------------------------
        # Contract subscriptions
        # ---------------------------------------------------------

        self._contract_queues: dict[
            int,
            asyncio.Queue,
        ] = {}

        # ---------------------------------------------------------
        # Concurrency
        # ---------------------------------------------------------

        self._send_lock = asyncio.Lock()

        self._connection_lock = asyncio.Lock()

        self._closed = False

    # ==================================================================
    # REST
    # ==================================================================

    @staticmethod
    def _masked_account_id(account_id: str) -> str:
        value = str(account_id or "")
        return f"…{value[-3:]}" if len(value) > 3 else "REDACTED"

    @staticmethod
    def _instance_label() -> str:
        return os.environ.get("INSTANCE_ID", "unconfigured")

    def _rest_headers(self) -> dict:
        return {
            "Deriv-App-ID": str(self.app_id),
            "Authorization": f"Bearer {self.api_token}",
        }

    def list_accounts(self) -> list[dict]:
        """
        List all Deriv accounts available to the API token.
        """

        response = requests.get(
            f"{REST_BASE_URL}/trading/v1/options/accounts",
            headers=self._rest_headers(),
            timeout=10,
        )

        response.raise_for_status()

        body = response.json()

        accounts = (
            body.get("data", body)
            if isinstance(body, dict)
            else body
        )

        print(f"[INSTANCE={self._instance_label()}] Deriv account inventory:")
        for account in accounts:
            print(
                f"  account={self._masked_account_id(account.get('account_id'))} "
                f"type={account.get('account_type', 'unknown')} "
                f"status={account.get('status', 'unknown')} "
                f"currency={account.get('currency', 'unknown')}"
            )

        self.accounts_by_id = {
            str(account["account_id"]): account
            for account in accounts
        }

        return accounts

    def request_otp(
        self,
        account_id: str,
    ) -> str:
        """
        Request a fresh authenticated WebSocket URL.

        OTPs are single-use and short-lived.
        """

        response = requests.post(
            f"{REST_BASE_URL}/trading/v1/options/accounts/"
            f"{account_id}/otp",
            headers=self._rest_headers(),
            timeout=10,
        )

        response.raise_for_status()

        body = response.json()

        data = body.get(
            "data",
            body,
        )

        ws_url = data.get("url")

        if not ws_url:
            raise RuntimeError(
                "Deriv OTP response did not contain "
                f"a WebSocket URL:\n{body}"
            )

        return ws_url

    # ==================================================================
    # CONNECTION
    # ==================================================================

    def _account_is_valid(
        self,
        account_id: str,
        allow_real: bool = False,
    ):
        account_key = str(account_id)

        account = self.accounts_by_id.get(
            account_key
        )

        if account is None:
            raise RuntimeError(
                f"Unknown account_id '{account_id}'. "
                "Call list_accounts() first."
            )

        account_type = account.get(
            "account_type"
        )

        if (
            account_type != "demo"
            and not allow_real
        ):
            raise RuntimeError(
                f"Refusing to connect: account "
                f"{account_id} has "
                f"account_type='{account_type}', "
                "not 'demo'."
            )

        return account_key

    @staticmethod
    def _is_websocket_open(ws) -> bool:
        """Support both legacy and modern websockets connection objects."""
        if ws is None:
            return False

        state = getattr(ws, "state", None)
        if state is not None:
            return getattr(state, "name", str(state)).upper() == "OPEN"

        return not getattr(ws, "closed", False)

    def _socket_healthy(self) -> bool:
        ws = self.websocket

        return (
            self._is_websocket_open(ws)
            and self._receiver_task is not None
            and not self._receiver_task.done()
        )

    async def connect(
        self,
        account_id: str,
        allow_real: bool = False,
    ):
        """
        Establish authenticated DEMO connection.

        Safe to call again after a disconnect.
        A fresh OTP is requested for every new
        authenticated WebSocket.
        """

        account_key = self._account_is_valid(
            account_id,
            allow_real,
        )

        self.account_id = account_key
        self._closed = False

        async with self._connection_lock:

            await self._replace_connection_locked()

        # Start keepalive exactly once.

        if (
            self._keepalive_task is None
            or self._keepalive_task.done()
        ):
            self._keepalive_task = (
                asyncio.create_task(
                    self._keepalive_loop()
                )
            )

    async def _replace_connection_locked(self):
        """
        Replace the current authenticated WebSocket.

        Caller must hold _connection_lock.
        """

        await self._stop_receiver()

        old_ws = self.websocket

        self.websocket = None

        if old_ws is not None:

            try:
                await old_ws.close()

            except Exception:
                pass

        if not self.account_id:
            raise RuntimeError(
                "No trading account configured."
            )

        if DEBUG_DERIV_API:
            print("Authenticating trading connection...")

        # IMPORTANT:
        # Never reuse an old OTP URL.
        ws_url = self.request_otp(
            self.account_id
        )

        if DEBUG_DERIV_API:
            print(
                f"Connecting to authenticated Deriv DEMO "
                f"account {self.account_id}..."
            )

        ws = await websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=60,
            close_timeout=10,
        )

        self.websocket = ws
        self._connection_generation += 1
        if self._connection_generation > 1:
            self.reconnect_count += 1
            event_type = "DERIV_RECONNECTED"
            message = "Authenticated Deriv WebSocket reconnected."
        else:
            event_type = "DERIV_AUTHENTICATED"
            message = "Authenticated Deriv WebSocket connected."
        self.authenticated = True
        self.last_authenticated_at = datetime.now(timezone.utc).isoformat()
        emit_event(
            event_type, component="deriv", message=message,
            details={
                "account_id": self._masked_account_id(self.account_id),
                "account_type": (self.accounts_by_id.get(self.account_id) or {}).get("account_type", "demo"),
                "reconnect_count": self.reconnect_count,
            },
        )

        print(
            f"[INSTANCE={self._instance_label()}] Authenticated: DEMO account "
            f"{self._masked_account_id(self.account_id)}"
        )

        # Exactly ONE receiver reads from this socket.

        self._receiver_task = (
            asyncio.create_task(
                self._receiver_loop(ws)
            )
        )

    async def _ensure_connected(self):
        """
        Make sure the authenticated trading socket is alive.

        This is called immediately before every request.
        """

        if self._closed:
            raise RuntimeError(
                "Deriv trading client is closed."
            )

        if self._socket_healthy():
            return

        if self.authenticated:
            self.authenticated = False
            emit_event("DERIV_DISCONNECTED", severity="WARN", component="deriv",
                       message="Authenticated Deriv WebSocket disconnected; reconnecting.")

        if not self.account_id:
            raise RuntimeError(
                "Trading client has no configured account. "
                "Call connect() first."
            )

        async with self._connection_lock:

            # Another coroutine may have repaired it
            # while we were waiting for the lock.

            if self._socket_healthy():
                return

            print("Trading connection lost — reconnecting...")

            await asyncio.wait_for(
                self._replace_connection_locked(),
                timeout=RECONNECT_TIMEOUT,
            )

            print("Trading connection restored.")

    async def _force_reconnect(self):
        """
        Force a fresh authenticated connection.
        """

        if (
            self._closed
            or not self.account_id
        ):
            return

        async with self._connection_lock:

            print(
                ">>> FORCING DERIV "
                "TRADING RECONNECT <<<"
            )

            await asyncio.wait_for(
                self._replace_connection_locked(),
                timeout=RECONNECT_TIMEOUT,
            )

            print(
                ">>> DERIV TRADING "
                "RECONNECTED <<<"
            )

    async def close(self):
        """
        Cleanly close everything.
        """

        self._closed = True

        await self._stop_keepalive()

        await self._stop_receiver()

        ws = self.websocket

        self.websocket = None

        if ws is not None:

            try:
                await ws.close()

            except Exception:
                pass

        self._fail_pending_requests(
            RuntimeError(
                "Deriv trading client was closed."
            )
        )

    async def _stop_keepalive(self):

        task = self._keepalive_task

        self._keepalive_task = None

        if (
            task is None
            or task.done()
        ):
            return

        task.cancel()

        try:
            await task

        except asyncio.CancelledError:
            pass

        except Exception:
            pass

    async def _stop_receiver(self):

        task = self._receiver_task

        self._receiver_task = None

        if (
            task is None
            or task.done()
        ):
            return

        task.cancel()

        try:
            await task

        except asyncio.CancelledError:
            pass

        except Exception:
            pass

    # ==================================================================
    # KEEPALIVE
    # ==================================================================

    async def _keepalive_loop(self):
        """
        Send application-level ping every 30 seconds.

        This prevents idle authenticated WebSocket connections
        from silently dying and lets Watch Tower reconnect before
        the next trade.
        """

        try:

            while not self._closed:

                await asyncio.sleep(
                    KEEPALIVE_INTERVAL
                )

                if self._closed:
                    return

                try:

                    await self._send(
                        {
                            "ping": 1,
                        },
                        timeout=PING_TIMEOUT,
                    )

                    if DEBUG_DERIV_API:
                        print("Trading WebSocket keepalive OK.")

                except asyncio.CancelledError:
                    raise

                except Exception as exc:

                    print()
                    print(
                        "!!! TRADING WS "
                        "KEEPALIVE FAILED !!!"
                    )

                    print(
                        f"Error: {exc}"
                    )

                    try:

                        await self._force_reconnect()

                    except Exception as reconnect_error:

                        print(
                            "!!! TRADING WS "
                            "RECONNECT FAILED !!!"
                        )

                        print(
                            f"Error: "
                            f"{reconnect_error}"
                        )

        except asyncio.CancelledError:
            return

    # ==================================================================
    # REQUEST IDS
    # ==================================================================

    def _get_req_id(self) -> int:

        req_id = self._next_req_id

        self._next_req_id += 1

        return req_id

    # ==================================================================
    # RECEIVER
    # ==================================================================

    async def _receiver_loop(
        self,
        ws,
    ):
        """
        The ONLY coroutine allowed to call websocket.recv().
        """

        try:

            while True:

                raw_message = await ws.recv()

                data = json.loads(
                    raw_message
                )

                req_id = data.get(
                    "req_id"
                )

                msg_type = data.get(
                    "msg_type"
                )

                # ------------------------------------------------------
                # API ERROR
                # ------------------------------------------------------

                if (
                    msg_type == "error"
                    or "error" in data
                ):

                    error = data.get(
                        "error",
                        {},
                    )

                    if req_id is not None:

                        future = (
                            self._pending_requests.pop(
                                int(req_id),
                                None,
                            )
                        )

                        if (
                            future is not None
                            and not future.done()
                        ):

                            future.set_exception(
                                RuntimeError(
                                    "Deriv API error: "
                                    f"{error}"
                                )
                            )

                    else:

                        print(
                            "\n!!! DERIV "
                            "ASYNC ERROR !!!"
                        )

                        print(
                            json.dumps(
                                data,
                                indent=2,
                            )
                        )

                    continue

                # ------------------------------------------------------
                # REQUEST / RESPONSE
                # ------------------------------------------------------

                was_request_response = False

                if req_id is not None:

                    future = (
                        self._pending_requests.pop(
                            int(req_id),
                            None,
                        )
                    )

                    if future is not None:

                        was_request_response = True

                        if not future.done():

                            future.set_result(
                                data
                            )

                # ------------------------------------------------------
                # CONTRACT SUBSCRIPTION
                # ------------------------------------------------------

                if (
                    msg_type
                    == "proposal_open_contract"
                ):

                    contract = data.get(
                        "proposal_open_contract",
                        {},
                    )

                    contract_id = contract.get(
                        "contract_id"
                    )

                    if contract_id is not None:

                        try:

                            contract_id = int(
                                contract_id
                            )

                        except (
                            TypeError,
                            ValueError,
                        ):
                            pass

                        queue = (
                            self._contract_queues.get(
                                contract_id
                            )
                        )

                        # Don't duplicate the initial
                        # subscription response.

                        if (
                            queue is not None
                            and not was_request_response
                        ):

                            await queue.put(
                                contract
                            )

        except asyncio.CancelledError:

            return

        except (
            websockets.exceptions.ConnectionClosed
        ) as exc:

            if self.websocket is ws:
                self.websocket = None

            print()
            print(
                "!!! DERIV TRADING "
                "WEBSOCKET CLOSED !!!"
            )

            print(
                f"Code: {exc.code}"
            )

            print(
                f"Reason: {exc.reason}"
            )

            self._fail_pending_requests(
                RuntimeError(
                    "Deriv trading WebSocket closed: "
                    f"{exc.code} {exc.reason}"
                )
            )

        except Exception as exc:

            if self.websocket is ws:
                self.websocket = None

            print()
            print(
                "!!! DERIV TRADING "
                "RECEIVER ERROR !!!"
            )

            print(
                f"{type(exc).__name__}: {exc}"
            )

            self._fail_pending_requests(
                exc
            )

    def _fail_pending_requests(
        self,
        exception: Exception,
    ):

        pending = list(
            self._pending_requests.items()
        )

        self._pending_requests.clear()

        for (
            _,
            future,
        ) in pending:

            if not future.done():

                future.set_exception(
                    exception
                )

    # ==================================================================
    # SEND / WAIT
    # ==================================================================

    async def _send(
        self,
        request: dict,
        expected_msg_type: str | None = None,
        timeout: float = 30.0,
    ) -> dict:
        """
        Send one request and wait for its exact req_id.

        If the connection died while Watch Tower was idle,
        reconnect before sending.

        IMPORTANT:
        This method does NOT blindly retry a request after
        it has been sent. A trading request could have reached
        Deriv even if the response was lost.
        """

        await self._ensure_connected()

        request = dict(request)

        if "req_id" not in request:

            request["req_id"] = (
                self._get_req_id()
            )

        req_id = int(
            request["req_id"]
        )

        future = (
            asyncio.get_running_loop()
            .create_future()
        )

        self._pending_requests[
            req_id
        ] = future

        try:

            if DEBUG_DERIV_API:
                print()
                print(">>> DERIV REQUEST")
                print(json.dumps(request, indent=2))

            async with self._send_lock:

                ws = self.websocket

                if not self._is_websocket_open(ws):

                    self._pending_requests.pop(
                        req_id,
                        None,
                    )

                    raise RuntimeError(
                        "Deriv trading WebSocket "
                        "closed before request "
                        "could be sent."
                    )

                await ws.send(
                    json.dumps(request)
                )

            response = await asyncio.wait_for(
                future,
                timeout=timeout,
            )

            if (
                expected_msg_type is not None
                and response.get(
                    "msg_type"
                )
                != expected_msg_type
            ):

                raise RuntimeError(
                    "Unexpected Deriv "
                    "response type. "
                    f"Expected "
                    f"'{expected_msg_type}', "
                    f"received "
                    f"'{response.get('msg_type')}'. "
                    f"Response: {response}"
                )

            if DEBUG_DERIV_API:
                print()
                print("<<< DERIV RESPONSE")
                print(json.dumps(response, indent=2))

            return response

        except asyncio.TimeoutError:

            self._pending_requests.pop(
                req_id,
                None,
            )

            raise RuntimeError(
                "Timed out waiting for "
                "Deriv response "
                f"for req_id={req_id}."
            )

        except Exception:

            self._pending_requests.pop(
                req_id,
                None,
            )

            raise

    # ==================================================================
    # BALANCE
    # ==================================================================

    async def get_balance(
        self,
    ) -> dict:

        return await self._send(
            {
                "balance": 1,
            },
            expected_msg_type="balance",
        )

    # ==================================================================
    # PORTFOLIO
    # ==================================================================

    async def get_portfolio(
        self,
    ) -> dict:
        """
        Return currently open contracts.

        This is particularly useful after an uncertain BUY
        connection failure.
        """

        return await self._send(
            {
                "portfolio": 1,
            },
            expected_msg_type="portfolio",
        )

    # ==================================================================
    # CONTRACT DISCOVERY
    # ==================================================================

    async def get_contracts_for(
        self,
        symbol: str,
    ) -> dict:

        return await self._send(
            {
                "contracts_for": symbol,
                "product_type": "multiplier",
            },
            expected_msg_type="contracts_for",
        )

    # ==================================================================
    # PROPOSAL
    # ==================================================================

    async def _request_multiplier_proposal(
        self,
        symbol: str,
        contract_type: str,
        stake: float,
        multiplier: float,
        currency: str,
    ) -> dict:

        request = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": currency,
            "duration_unit": "s",
            "multiplier": multiplier,
            "underlying_symbol": symbol,
        }

        # A proposal is read-only.
        #
        # Therefore it is safe to reconnect and retry it
        # if the connection died before the proposal response.

        try:

            response = await self._send(
                request,
                expected_msg_type="proposal",
            )

        except Exception as exc:

            message = str(
                exc
            ).lower()

            connection_error = any(
                phrase in message
                for phrase in (
                    "closed",
                    "not connected",
                    "connection",
                    "websocket",
                )
            )

            if not connection_error:
                raise

            print()
            print(
                "!!! PROPOSAL REQUEST "
                "LOST WITH CONNECTION !!!"
            )

            print(
                ">>> RECONNECTING BEFORE "
                "RETRYING PROPOSAL <<<"
            )

            await self._force_reconnect()

            response = await self._send(
                request,
                expected_msg_type="proposal",
            )

        proposal = response.get(
            "proposal",
            {},
        )

        proposal_id = proposal.get(
            "id"
        )

        if not proposal_id:

            raise RuntimeError(
                "Deriv returned a proposal "
                "response without a proposal ID:\n"
                f"{response}"
            )

        return response

    # ==================================================================
    # BUY MULTIPLIER
    # ==================================================================

    async def buy_multiplier(
        self,
        symbol: str,
        direction: str,
        stake: float,
        multiplier: float,
        stop_loss: float,
        take_profit: float,
        currency: str = "USD",
        reference_quote: float | None = None,
        max_quote_deviation: float | None = None,
    ) -> dict:
        """
        Buy a multiplier contract:

            proposal
                ->
            buy
                ->
            contract_update

        direction:
            "up"   -> MULTUP
            "down" -> MULTDOWN

        IMPORTANT:

        The proposal can safely be retried after a connection
        failure because it does not create a position.

        The actual BUY is NEVER blindly retried.

        If the BUY request reaches Deriv but the response is
        lost, retrying could create a duplicate contract.
        """

        direction = (
            direction
            .lower()
            .strip()
        )

        if direction not in (
            "up",
            "down",
        ):

            raise ValueError(
                "direction must be "
                "'up' or 'down'"
            )

        contract_type = (
            "MULTUP"
            if direction == "up"
            else "MULTDOWN"
        )

        # -------------------------------------------------------------
        # 1. PROPOSAL
        # -------------------------------------------------------------

        print(f"[{symbol}] Executing trade: {contract_type}")

        try:
            proposal_response = (
                await self._request_multiplier_proposal(
                    symbol=symbol,
                    contract_type=contract_type,
                    stake=stake,
                    multiplier=multiplier,
                    currency=currency,
                )
            )
        except Exception as exc:
            error_text = str(exc)
            if (
                "MultiplierOutOfRange" in error_text
                or "Multiplier is not in acceptable range" in error_text
            ):
                allowed = []
                match = re.search(r"Accepts\s+([^}\]]+)", error_text)
                if match:
                    allowed = [
                        float(value)
                        for value in re.findall(
                            r"\d+(?:\.\d+)?", match.group(1)
                        )
                    ]
                if not allowed:
                    match = re.search(
                        r"code_args.*?\[(.*?)\]",
                        error_text,
                    )
                    if match:
                        allowed = [
                            float(value)
                            for value in re.findall(
                                r"\d+(?:\.\d+)?",
                                match.group(1),
                            )
                        ]
                if allowed:
                    raise MultiplierOutOfRangeError(
                        symbol=symbol,
                        requested=multiplier,
                        allowed=sorted(set(allowed)),
                    ) from exc
            raise

        proposal = (
            proposal_response.get(
                "proposal",
                {},
            )
        )

        proposal_id = proposal.get(
            "id"
        )

        if DEBUG_DERIV_API:
            print(f"Proposal ID: {proposal_id}")
            print(f"Ask price: {proposal.get('ask_price')}")
            print(f"Spot: {proposal.get('spot')}")

        # Recheck the broker's freshest underlying quote after proposal
        # generation and immediately before the irreversible BUY request.
        proposal_spot = proposal.get("spot")
        if reference_quote is not None and max_quote_deviation is not None and proposal_spot is not None:
            try:
                spot_value = float(proposal_spot)
                reference_value = float(reference_quote)
                moved = abs(spot_value - reference_value)
            except (TypeError, ValueError):
                moved = None
            if moved is None or not math.isfinite(moved):
                raise ExecutionPriceMovedError("Deriv proposal returned an unusable spot quote.")
            if moved is not None and moved > max_quote_deviation:
                raise ExecutionPriceMovedError(
                    f"Proposal spot moved {moved:g} from the entry tick; "
                    f"maximum allowed is {max_quote_deviation:g}."
                )

        # -------------------------------------------------------------
        # 2. BUY
        # -------------------------------------------------------------

        print(
            f"[{symbol}] Buying {contract_type} | "
            f"stake=${stake:.2f} | multiplier={multiplier}"
        )

        try:

            buy_response = await self._send(
                {
                    "buy": proposal_id,
                    "price": stake,
                },
                expected_msg_type="buy",
            )

        except Exception as exc:

            # A multiplier validation error is a broker-side rejection.
            # No contract is created, so it is safe for the caller to
            # select another accepted multiplier and start a fresh
            # proposal/buy cycle. Do NOT reconnect/portfolio-check this
            # case because it is not an uncertain BUY.
            error_text = str(exc)
            if "MultiplierOutOfRange" in error_text or "Multiplier is not in acceptable range" in error_text:
                allowed = []
                match = re.search(r"Accepts\s+([^}\]]+)", error_text)
                if match:
                    for value in re.findall(r"\d+(?:\.\d+)?", match.group(1)):
                        allowed.append(float(value))
                if not allowed:
                    match = re.search(r"code_args.*?\[(.*?)\]", error_text)
                    if match:
                        allowed = [float(v) for v in re.findall(r"\d+(?:\.\d+)?", match.group(1))]
                if allowed:
                    raise MultiplierOutOfRangeError(
                        symbol=symbol,
                        requested=multiplier,
                        allowed=sorted(set(allowed)),
                    ) from exc

            print()
            print(
                "!!! BUY REQUEST FAILED "
                "/ CONNECTION LOST !!!"
            )
            print(f"Error: {exc}")

            print(
                "IMPORTANT: Watch Tower will "
                "NOT blindly retry the BUY."
            )

            print(
                "The trading connection will "
                "be restored and the portfolio "
                "will be checked."
            )

            # Reconnect and inspect open positions.
            #
            # We deliberately do NOT send another BUY.

            try:

                await self._force_reconnect()

                portfolio = (
                    await self.get_portfolio()
                )

                if DEBUG_DERIV_API:
                    print("Portfolio after uncertain BUY:")
                    print(json.dumps(portfolio, indent=2))

            except Exception as verify_error:

                print()

                print(
                    "!!! COULD NOT VERIFY "
                    "PORTFOLIO AFTER "
                    "BUY FAILURE !!!"
                )

                print(
                    f"Verification error: "
                    f"{verify_error}"
                )

            raise

        buy = buy_response.get(
            "buy",
            {},
        )

        contract_id = buy.get(
            "contract_id"
        )

        transaction_id = buy.get(
            "transaction_id"
        )

        if not contract_id:

            raise RuntimeError(
                "Deriv BUY response did not "
                "contain a contract_id:\n"
                f"{buy_response}"
            )

        print(
            f"[{symbol}] Contract opened: {contract_id} | "
            f"transaction={transaction_id}"
        )

        if DEBUG_DERIV_API:
            print(f"Buy price: {buy.get('buy_price')}")
            print(f"Payout: {buy.get('payout')}")

        # -------------------------------------------------------------
        # 3. APPLY SL / TP
        # -------------------------------------------------------------

        print(f"[{symbol}] Applying Deriv monetary stop/target orders...")

        try:

            update_response = (
                await self._send(
                    {
                        "contract_update": 1,
                        "contract_id": contract_id,
                        "limit_order": {
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                        },
                    },
                    expected_msg_type=(
                        "contract_update"
                    ),
                )
            )

        except Exception as update_error:

            print()

            print(
                "!!! FAILED TO APPLY "
                "Deriv monetary stop/target orders !!!"
            )

            print(
                f"Error: "
                f"{update_error}"
            )

            print()

            print(
                "!!! ATTEMPTING TO CLOSE "
                "UNPROTECTED CONTRACT !!!"
            )

            try:

                close_response = (
                    await self.close_contract(
                        int(contract_id)
                    )
                )

                print(
                    "Unprotected "
                    "contract closed."
                )

                print(
                    f"Close response: "
                    f"{close_response.get('sell', close_response)}"
                )

            except Exception as close_error:

                print(
                    "!!! WARNING: FAILED "
                    "TO CLOSE UNPROTECTED "
                    "CONTRACT !!!"
                )

                print(
                    f"Close error: "
                    f"{close_error}"
                )

            raise RuntimeError(
                "Contract was purchased but Deriv "
                "monetary stop/target orders could not be applied. "
                "The client attempted to close "
                "the contract. "
                f"Original error: "
                f"{update_error}"
            ) from update_error

        contract_update = (
            update_response.get(
                "contract_update",
                {},
            )
        )

        print(
            f"[{symbol}] Deriv limits accepted | "
            f"deriv_stop_loss_amount={stop_loss} "
            f"broker_stop_loss_value="
            f"{broker_value(contract_update.get('stop_loss'))} | "
            f"deriv_take_profit_amount={take_profit} "
            f"broker_take_profit_value="
            f"{broker_value(contract_update.get('take_profit'))}"
        )

        # Preserve live_trader.py expectation.

        buy_response[
            "contract_update"
        ] = contract_update

        print()

        print(
            "=" * 60
        )

        print(
            f"[{symbol}] BUY {contract_type} "
            f"stake={stake} "
            f"multiplier={multiplier} "
            f"deriv_stop_loss_amount={stop_loss} "
            f"deriv_take_profit_amount={take_profit}"
        )

        print(
            "=" * 60
        )

        return buy_response

    # ==================================================================
    # CLOSE CONTRACT
    # ==================================================================

    async def close_contract(
        self,
        contract_id: int,
    ) -> dict:
        """
        Close an open position immediately.
        """

        print(f"Closing contract {contract_id}...")

        response = await self._send(
            {
                "sell": int(contract_id),
                "price": 0,
            },
            expected_msg_type="sell",
        )

        sell = response.get(
            "sell",
            {},
        )

        print(f"Contract {contract_id} closed.")
        if DEBUG_DERIV_API:
            print(f"Close response: {sell}")

        return response

    # ==================================================================
    # CONTRACT SUBSCRIPTION
    # ==================================================================

    async def subscribe_contract(
        self,
        contract_id: int,
    ):
        """
        Yield proposal_open_contract updates.

        _receiver_loop() remains the only reader.
        """

        contract_id = int(
            contract_id
        )

        queue = asyncio.Queue()

        self._contract_queues[
            contract_id
        ] = queue

        request = {
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
        }

        try:

            while True:

                generation_before_request = (
                    self._connection_generation
                )
                initial_response = (
                    await self._send(
                        request,
                        expected_msg_type=(
                            "proposal_open_contract"
                        ),
                    )
                )
                response_generation = (
                    self._connection_generation
                )

                # If this response raced a reconnect, discard it and ask the
                # newly authenticated socket for the current contract state.
                if (
                    generation_before_request
                    != response_generation
                ):
                    continue

                initial_contract = (
                    initial_response.get(
                        "proposal_open_contract",
                        {},
                    )
                )

                if initial_contract:

                    yield initial_contract

                    if initial_contract.get(
                        "is_sold"
                    ):
                        return

                subscription_generation = response_generation
                if (
                    subscription_generation
                    != self._connection_generation
                ):
                    continue

                next_reconcile_at = (
                    asyncio.get_running_loop().time()
                    + CONTRACT_RECONCILE_INTERVAL
                )

                while True:

                    try:
                        contract = await asyncio.wait_for(
                            queue.get(),
                            timeout=min(
                                5.0,
                                max(
                                    0.0,
                                    next_reconcile_at
                                    - asyncio.get_running_loop().time(),
                                ),
                            ),
                        )
                    except asyncio.TimeoutError:
                        # A reconnect drops Deriv's server-side subscription.
                        # Leave this wait and request a fresh subscription below.
                        if (
                            subscription_generation
                            != self._connection_generation
                            or not self._socket_healthy()
                        ):
                            break

                        if (
                            asyncio.get_running_loop().time()
                            >= next_reconcile_at
                        ):
                            snapshot_generation = self._connection_generation
                            try:
                                snapshot_response = await self._send(
                                    {
                                        "proposal_open_contract": 1,
                                        "contract_id": contract_id,
                                    },
                                    expected_msg_type=(
                                        "proposal_open_contract"
                                    ),
                                )
                            except Exception as exc:
                                # A failed status check is not evidence that
                                # the contract closed. Keep the slot reserved
                                # and retry after the next interval.
                                if (
                                    subscription_generation
                                    != self._connection_generation
                                    or not self._socket_healthy()
                                ):
                                    break
                                print(
                                    "Contract status reconciliation failed "
                                    f"for {contract_id}: {exc}"
                                )
                            else:
                                if (
                                    snapshot_generation
                                    != self._connection_generation
                                ):
                                    break

                                snapshot = snapshot_response.get(
                                    "proposal_open_contract",
                                    {},
                                )
                                if snapshot:
                                    yield snapshot
                                    if snapshot.get("is_sold"):
                                        return

                            next_reconcile_at = (
                                asyncio.get_running_loop().time()
                                + CONTRACT_RECONCILE_INTERVAL
                            )
                        continue

                    yield contract

                    if contract.get(
                        "is_sold"
                    ):
                        return

        finally:

            self._contract_queues.pop(
                contract_id,
                None,
            )
