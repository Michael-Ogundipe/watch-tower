import asyncio
import websockets


async def test():
    url = "wss://api.derivws.com/trading/v1/options/ws/public"

    print("Connecting...")

    async with websockets.connect(url, open_timeout=10) as websocket:
        print("CONNECTED!")


asyncio.run(test())