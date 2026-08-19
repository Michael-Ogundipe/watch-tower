import asyncio
import websockets
import json

from parser import parse_tick


DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


async def main():
    async with websockets.connect(DERIV_WS_URL) as websocket:
        print("Connected to Deriv!")
        request = {
            "ticks": "R_75",
            "subscribe": 1
        }

        await websocket.send(json.dumps(request))

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            tick = parse_tick(data)
            print(tick)


asyncio.run(main()) 