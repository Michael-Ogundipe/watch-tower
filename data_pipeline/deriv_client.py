import asyncio
import json

import websockets

from models import Timeframe


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
    ):
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

        return data


async def main():
    client = DerivClient()

    await client.connect()

    candles = await client.get_candles(
        symbol="R_75",
        timeframe=Timeframe.H4,
        count=10,
    )

    print(json.dumps(candles, indent=2))


if __name__ == "__main__":
    asyncio.run(main())