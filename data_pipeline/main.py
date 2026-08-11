import asyncio
import websockets


DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


async def main():
    async with websockets.connect(DERIV_WS_URL) as websocket:
        print("Connected to Deriv!")


asyncio.run(main()) 