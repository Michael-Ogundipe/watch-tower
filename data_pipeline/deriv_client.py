import asyncio
import json
from datetime import datetime, timezone
import websockets

from models import Candle, Timeframe
from parser import parse_candles, parse_tick
from candle_engine import CandleEngine
from ops_observability import emit_event


DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


class DerivClient:
    def __init__(self):
        self.websocket = None
        self.connected = False
        self.ever_connected = False
        self.reconnect_count = 0
        self.last_disconnect_at = None
        self.last_reconnect_at = None

    async def connect(self):
        try:
            self.websocket = await websockets.connect(DERIV_WS_URL, ping_interval=20,
            ping_timeout=60,)
        except Exception as exc:
            self.connected = False
            emit_event("MARKET_DATA_CONNECT_FAILED", severity="ERROR", component="market_data",
                       message="Market data WebSocket connection failed.", reason=type(exc).__name__)
            raise

        self.connected = True
        if self.ever_connected:
            self.reconnect_count += 1
            self.last_reconnect_at = datetime.now(timezone.utc).isoformat()
            emit_event("MARKET_DATA_RECONNECTED", component="market_data",
                       message="Market data WebSocket reconnected.",
                       details={"reconnect_count": self.reconnect_count})
        else:
            emit_event("MARKET_DATA_CONNECTED", component="market_data",
                       message="Market data WebSocket connected.")
        self.ever_connected = True

        print("Connected to Deriv!")

    
    async def get_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
        count: int,
    ) -> list[Candle]:

        all_candles = []

        remaining = count

        end = "latest"

        while remaining > 0:

            batch_count = min(remaining, 1000)

            request = {
                "ticks_history": symbol,
                "end": end,
                "count": batch_count,
                "style": "candles",
                "granularity": timeframe.value,
            }

            await self.websocket.send(
                json.dumps(request)
            )

            message = await self.websocket.recv()

            data = json.loads(message)

            if "error" in data:
                raise Exception(
                    f"Deriv error: {data['error']}"
                )

            candles = parse_candles(
                data,
                symbol,
                timeframe,
            )

            if not candles:
                break

            all_candles.extend(candles)

            remaining -= len(candles)

            # Oldest candle in this batch
            # becomes the end of the next request.
            # NOTE: the current ticks_history schema requires `end` to be a
            # string ("latest" or an epoch string) -- an int here would
            # serialize as a JSON number and can fail validation.
            end = str(
                int(candles[0].timestamp.timestamp()) - timeframe.value
            )

            # Prevent infinite loops
            if len(candles) < batch_count:
                break

        # Remove duplicates
        unique_candles = {
            candle.timestamp: candle
            for candle in all_candles
        }

        candles = sorted(
            unique_candles.values(),
            key=lambda candle: candle.timestamp,
        )

        return candles

    async def close(self):
        """Close the websocket connection if one is open."""
        websocket = self.websocket
        self.websocket = None

        if websocket is not None:
            await websocket.close()

    async def subscribe_ticks(self, symbol: str):
        while True:
            try:
                request = {
                    "ticks": symbol,
                    "subscribe": 1,
                }

                await self.websocket.send(json.dumps(request))

                while True:
                    message = await self.websocket.recv()
                    data = json.loads(message)

                    if data.get("msg_type") != "tick":
                        continue

                    yield parse_tick(data)

            except websockets.exceptions.ConnectionClosed:
                self.connected = False
                self.last_disconnect_at = datetime.now(timezone.utc).isoformat()
                emit_event("MARKET_DATA_DISCONNECTED", severity="WARN", component="market_data",
                           symbol=symbol, message="Market data WebSocket disconnected; reconnecting.")
                print("Deriv connection lost. Reconnecting...")

                await asyncio.sleep(3)

                try:
                    await self.connect()
                except Exception as error:
                    self.connected = False
                    emit_event("MARKET_DATA_RECONNECT_FAILED", severity="ERROR", component="market_data",
                               symbol=symbol, message="Market data reconnect attempt failed.",
                               reason=type(error).__name__)
                    print(f"Reconnect failed: {error}")
                    await asyncio.sleep(5)
