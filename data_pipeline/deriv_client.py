import asyncio
import json

import websockets

from models import Candle, Timeframe
from parser import parse_candles, parse_tick
from parser import parse_tick


DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


class DerivClient:
    def __init__(self):
        self.websocket = None

    async def connect(self):
        self.websocket = await websockets.connect(DERIV_WS_URL)
        print("Connected to Deriv!")

    
    async def get_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
        count: int,
    ) -> list[Candle]:
        request = {
            "ticks_history": symbol,
            "end": "latest",
            "count": count,
            "style": "candles",
            "granularity": timeframe.value,
        }

        await self.websocket.send(json.dumps(request))

        message = await self.websocket.recv()

        data = json.loads(message)

        return parse_candles(
            data,
            symbol,
            timeframe,
        )

    async def subscribe_ticks(self, symbol: str):
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


async def main():
    client = DerivClient()

    await client.connect()

    # candles = await client.get_candles(
    #     symbol="R_75",
    #     timeframe=Timeframe.H4,
    #     count=10,
    # )

    # for candle in candles:
    #     print(candle)

    async for tick in client.subscribe_ticks("R_75"):
        print(tick)


if __name__ == "__main__":
    asyncio.run(main())

