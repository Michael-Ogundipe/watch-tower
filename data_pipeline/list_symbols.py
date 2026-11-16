"""
Run this BEFORE adding a new symbol to config.SYMBOLS.

It only calls the public, read-only `active_symbols` endpoint (no auth,
no risk to your account) and prints the exact API symbol code for every
market whose display name matches what you're looking for.

This matters because Deriv's API symbol codes don't always match what
people call the market colloquially -- e.g. "Volatility 75 Index" (what
traders call "V_75") is actually "R_75" on the wire.

Usage:
    python list_symbols.py               # prints Jump + Volatility 75/10 matches
    python list_symbols.py "Jump 25"     # prints matches for your own search term(s)
"""

import asyncio
import json
import sys

import websockets

DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DEFAULT_SEARCH_TERMS = [
    "Jump 75",
    "Jump 10",
    "Volatility 75",
]


async def list_symbols(search_terms):
    print("Connecting...")

    async with websockets.connect(DERIV_WS_URL, open_timeout=10) as websocket:
        print("CONNECTED!\n")

        await websocket.send(
            json.dumps({"active_symbols": "brief"})
        )

        message = await websocket.recv()
        data = json.loads(message)

        if "error" in data:
            print("ERROR:", data["error"])
            return

        matches = []

        for symbol in data["active_symbols"]:
            name = symbol.get("underlying_symbol_name", "")

            if any(term.lower() in name.lower() for term in search_terms):
                matches.append(
                    (symbol["underlying_symbol"], name)
                )

        if not matches:
            print("No matches found for:", ", ".join(search_terms))
            return

        print(f"{'API symbol code':<18} display name")
        print("-" * 50)

        for code, name in sorted(matches, key=lambda m: m[1]):
            print(f"{code:<18} {name}")


if __name__ == "__main__":
    terms = sys.argv[1:] or DEFAULT_SEARCH_TERMS

    asyncio.run(list_symbols(terms))
